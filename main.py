import asyncio
import os
from collections import defaultdict
import re
from typing import Any, Dict, Optional, List
from urllib.parse import urljoin

import aiofiles
import httpx
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress
import xlsxwriter
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

load_dotenv()

# Globals runtime
JWT: str = ""
console = Console()
error_console = Console(stderr=True, style="bold red")

# Env
ELECTION_ID = os.getenv("ELECTION_ID")
BASE_API_URL = os.getenv("BASE_API_URL", "")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
# ----------------------------------------------------------------------------------

# Tunables
DOWNLOAD_ATTACHMENTS = os.getenv("DOWNLOAD_ATTACHMENTS", "").lower() == "true"
CONCURRENT_WORKERS = int(os.getenv("CONCURRENT_WORKERS", "8"))
HTTP_TIMEOUT_SECONDS = None  # unlimited; set number if desired
# ----------------------------------------------------------------------------------

# Paths
EXPORT_ROOT = os.path.join("exported-data", ELECTION_ID)
os.makedirs(EXPORT_ROOT, exist_ok=True)
# ----------------------------------------------------------------------------------

# Google
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
# ----------------------------------------------------------------------------------

# ---------------------------
# Utilities
# ---------------------------
def clean_sheet_name(name: str) -> str:
    # Remove invalid characters
    name = re.sub(r'[\[\]\:\*\?\/\\]', '', name)
    # Trim to max length (31)
    return name[:31].strip()


def local_submission_attachment_path(submission_id: str, attachment: Dict[str, Any]) -> str:
    return os.path.abspath(os.path.join(EXPORT_ROOT, f"submission-{submission_id}", attachment["uploadedFileName"]))


def local_quick_report_attachment_path(quick_report_id: str, attachment: Dict[str, Any]) -> str:
    return os.path.abspath(
        os.path.join(EXPORT_ROOT, f"quick-reports-{quick_report_id}", attachment["uploadedFileName"]))


# ---------------------------
# Fetch and download helpers
# ---------------------------
async def fetch_json(client: httpx.AsyncClient, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


async def download_binary(client: httpx.AsyncClient, url: str, target_path: str) -> None:
    # Write binary to disk with aiofiles to avoid blocking loop
    if os.path.exists(target_path):
        return
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    resp = await client.get(url)
    resp.raise_for_status()
    async with aiofiles.open(target_path, "wb") as f:
        await f.write(resp.content)


# ---------------------------
# Auth
# ---------------------------
async def log_in(client: httpx.AsyncClient) -> None:
    global JWT
    url = urljoin(BASE_API_URL, "/api/auth/login")
    data = {"email": os.getenv("ADMIN_EMAIL") or ADMIN_EMAIL, "password": os.getenv("ADMIN_PASSWORD") or ADMIN_PASSWORD}
    resp = await client.post(url, json=data)
    resp.raise_for_status()
    JWT = resp.json().get("token")
    # Attach token header to client for reuse
    client.headers.update({"Authorization": f"Bearer {JWT}"})
    console.log("Logged in successfully.")


# ---------------------------
# Download full objects & attachments
# ---------------------------
async def fetch_submission_detail(client: httpx.AsyncClient, submission_id: str, sem: asyncio.Semaphore, progress,
                                  task) -> Dict[str, Any]:
    url = urljoin(BASE_API_URL, f"/api/election-rounds/{ELECTION_ID}/form-submissions/{submission_id}:v2")

    async with sem:
        try:
            submission = await fetch_json(client, url)
            progress.update(task, advance=1)
            return submission
        except Exception as e:
            error_console.log(f"Failed to download submission {submission_id}: {e}")


async def fetch_form_detail(client: httpx.AsyncClient, form_id: str, progress,
                            task) -> Dict[str, Any]:
    # console.log(f"Downloading form {form_id}")
    url = urljoin(BASE_API_URL, f"/api/election-rounds/{ELECTION_ID}/forms/{form_id}")
    form = await fetch_json(client, url)
    progress.update(task, advance=1)
    return form


async def fetch_quick_report_detail(client: httpx.AsyncClient, quick_report_id: str, progress,
                                    task) -> Dict[str, Any]:
    # console.log(f"Downloading quick report {quick_report_id}")
    url = urljoin(BASE_API_URL, f"/api/election-rounds/{ELECTION_ID}/quick-reports/{quick_report_id}")
    quick_report = await fetch_json(client, url)
    progress.update(task, advance=1)
    return quick_report


# ---------------------------
# Pagination helper
# ---------------------------
async def fetch_all_paginated(client: httpx.AsyncClient, params_extra: Optional[Dict[str, Any]] = None) -> List[
    Dict[str, Any]]:
    url = urljoin(BASE_API_URL, f"/api/election-rounds/{ELECTION_ID}/form-submissions:byEntry")

    page_number = 1
    page_size = 100
    all_items: List[Dict[str, Any]] = []
    params_extra = params_extra or {}
    while True:
        params = {**params_extra, "pageNumber": page_number, "pageSize": page_size, "dataSource": "Coalition"}
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        all_items.extend(items)
        if len(items) < page_size:
            break
        page_number += 1
    return all_items


def get_question_answer(question, answers, attachments_by_question, notes_by_question,
                        default_language):
    question_id = question["id"]
    answer = next((a for a in answers if a["questionId"] == question_id), {})
    notes = "\n\n\n".join(notes_by_question.get(question_id, []))
    attachments = "\n\n".join(attachments_by_question.get(question_id, []))
    has_free_text_option = any(
        opt.get("isFreeText", False)
        for opt in question.get("options", [])
    )

    question_type = question.get("$questionType", "")
    if question_type == "textQuestion":
        if not answer or not answer.get("text"):
            return ["", notes, attachments]
        else:
            return [answer["text"], notes, attachments]
    if question_type == "numberQuestion":
        if not answer or not answer.get("value"):
            return ["", notes, attachments]
        else:
            return [answer["value"], notes, attachments]
    if question_type == "dateQuestion":
        if not answer or not answer.get("date"):
            return ["", notes, attachments]
        else:
            date = datetime.fromisoformat(answer["date"].replace("Z", "+00:00"))
            return [date.strftime("%Y-%m-%d %H:%M"), notes, attachments]
    if question_type == "singleSelectQuestion":
        if not answer or not answer.get("selection"):
            if has_free_text_option:
                return ["", "", notes, attachments]
            else:
                return ["", notes, attachments]
        else:
            option = next(
                (o for o in question["options"]
                 if o["id"] == answer["selection"]["optionId"]),
                None
            )
            selection = option["text"][default_language] if option else ""
            if has_free_text_option:
                return [selection, answer["selection"].get("text", ""), notes, attachments]
            else:
                return [selection, notes, attachments]
    if question_type == "multiSelectQuestion":
        if not answer or not answer.get("selection"):
            if has_free_text_option:
                return ["", "", notes, attachments]
            else:
                return ["", notes, attachments]
        else:
            # Build selection strings robustly
            sel_texts = []
            for sel in answer["selection"]:
                opt_id = sel.get("optionId")
                if not opt_id:
                    continue
                opt = next((o for o in question["options"] if o["id"] == opt_id), None)
                if opt:
                    sel_texts.append(opt["text"][default_language])
            selection = ", ".join(sel_texts)
            if has_free_text_option:
                return [selection, ", ".join(sel.get("text", "") for sel in answer["selection"] if sel.get("text")),
                        notes, attachments]
            else:
                return [selection, notes, attachments]
    if question_type == "ratingQuestion":
        if not answer or not answer.get("value"):
            return ["", notes, attachments]
        else:
            return [answer["value"], notes, attachments]
    return ["unknown value", notes, attachments]


def prepare_data_for_export(forms: List[Dict[str, Any]], submissions: List[Dict[str, Any]]):
    default_headers = [
        "SubmissionId",
        "TimeSubmitted",
        "FollowUpStatus",
        "Level1",
        "Level2",
        "Level3",
        "Level4",
        "Level5",
        "Number",
        "Ngo",
        "MonitoringObserverId",
        "Name",
        "Email",
        "PhoneNumber"
    ]

    forms_data = {}
    forms.sort(
            key=lambda f: (
                    0 if f["name"][f["defaultLanguage"]].strip().lower() == "psi" else 1,
                    f["name"][f["defaultLanguage"]].strip().lower()
            )
    )

    for idx, form in enumerate(forms, start=1):
        form_headers = list(default_headers)

        for question in form["questions"]:
            form_headers.append(f"{question['code']} - {question['text'][form['defaultLanguage']]}")
            if question.get("$questionType") in ("singleSelectQuestion", "multiSelectQuestion") and any(
                    opt.get("isFreeText") for opt in question.get("options", [])):
                form_headers.append("FreeText")
            form_headers.append("Notes")
            form_headers.append("Attachments")

        form_submissions = [sub for sub in submissions if sub.get("formId") == form.get("id")]

        data = [form_headers]
        for fs in form_submissions:
            row_data = [
                fs.get("submissionId", ""),
                fs.get("timeSubmitted", ""),
                fs.get("followUpStatus", ""),
                fs.get("level1", ""),
                fs.get("level2", ""),
                fs.get("level3", ""),
                fs.get("level4", ""),
                fs.get("level5", ""),
                fs.get("number", ""),
                fs.get("ngo", ""),
                fs.get("monitoringObserverId", ""),
                fs.get("observerName", ""),
                fs.get("email", ""),
                fs.get("phoneNumber", "")
            ]

            attachments_by_question = defaultdict(list)
            for attachment in fs.get("attachments", []):
                attachments_by_question[attachment["questionId"]].append(attachment["presignedUrl"])

            notes_by_question = defaultdict(list)
            for note in fs.get("notes", []):
                notes_by_question[note["questionId"]].append(note["text"])

            for question in form["questions"]:
                answers = fs.get("answers", [])
                row = get_question_answer(question, answers, attachments_by_question, notes_by_question,
                                          form["defaultLanguage"])
                row_data.extend(row)

            data.append(row_data)

        sheet_name = clean_sheet_name(
            f"{idx}_PSI" if form["formType"] == 'PSI'
            else f"{idx}_{form['name'][form['defaultLanguage']]}"
        )

        forms_data[sheet_name] = data
    return forms_data


async def write_submissions_to_excel(forms_data):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    filename = f"form_submissions_{timestamp}.xlsx"
    path = os.path.join(EXPORT_ROOT, filename)

    workbook = xlsxwriter.Workbook(path)

    for sheet_name, data in forms_data.items():
        form_worksheet = workbook.add_worksheet(sheet_name)

        # write data
        for row_idx, row_data in enumerate(data):
            for col_idx, cell_value in enumerate(row_data):
                form_worksheet.write_string(row_idx, col_idx, str(cell_value or ""))

    workbook.close()
    console.log(f"Exported submissions to {path}")


#
async def write_submissions_to_google_spreadsheet(forms_data):
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_PATH, scopes=scopes)
    client = gspread.authorize(credentials)
    workbook = client.open_by_key(GOOGLE_SHEET_ID)

    for sheet_name, data in forms_data.items():
        # Clean up name and ensure valid length
        num_rows = max(1000, len(data))
        num_cols = max(1, len(data[0]) if data else 1)

        # Try to get existing worksheet or create a new one
        try:
            worksheet = workbook.worksheet(sheet_name)
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            worksheet = workbook.add_worksheet(title=sheet_name, rows=num_rows, cols=num_cols)

        # Convert all cells to strings
        values = [[str(cell or "") for cell in row] for row in data]

        # 🚀 Bulk update in one request
        worksheet.update(range_name="A1", values=values)

        console.log(f"[green]Updated Google Sheet tab:[/] {sheet_name} ({num_rows} rows, {num_cols} cols)")

    console.log("[cyan]All forms successfully uploaded to Google Sheets.[/]")


# Attachment download worker with semaphore
async def download_submission_attachment_worker(client: httpx.AsyncClient, submission: Dict[str, Any],
                                                attachment: Dict[str, Any], sem: asyncio.Semaphore) -> None:
    submission_id = submission["submissionId"]
    path = local_submission_attachment_path(submission_id, attachment)
    async with sem:
        try:
            await download_binary(client, attachment["presignedUrl"], path)
        except Exception as e:
            error_console.log(
                f"Failed to download attachment {attachment.get('uploadedFileName')} for submission {submission_id}: {e}")


async def main():
    if not ELECTION_ID:
        error_console.log("ELECTION_ID env var not set.")
        return

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS) as client:
        await log_in(client)

        with Progress(console=console, transient=True) as progress:
            task_overall = progress.add_task("[cyan]Overall progress...", total=4)

            # 1️⃣ Fetch submission list
            console.log("Fetching submission list...")
            submissions_list = await fetch_all_paginated(client)
            progress.update(task_overall, advance=1)

            # 2️⃣ Fetch submission details concurrently
            sem_submissions = asyncio.Semaphore(CONCURRENT_WORKERS)

            task_submissions = progress.add_task("[green]Fetching submissions...", total=len(submissions_list))

            submission_tasks = [
                fetch_submission_detail(client, s["submissionId"], sem_submissions, progress, task_submissions)
                for s in submissions_list
            ]
            submission_details = [r for r in (await asyncio.gather(*submission_tasks)) if r]
            progress.update(task_overall, advance=1)

            # 3️⃣ Fetch distinct forms
            form_ids = {s.get("formId") for s in submission_details if s.get("formId")}

            task_forms = progress.add_task("[magenta]Fetching forms...", total=len(form_ids))

            form_tasks = [fetch_form_detail(client, fid, progress, task_forms) for fid in form_ids]
            forms = [f for f in (await asyncio.gather(*form_tasks)) if f]
            progress.update(task_overall, advance=1)

            if DOWNLOAD_ATTACHMENTS:
                sem_attach = asyncio.Semaphore(CONCURRENT_WORKERS)
                attach_tasks = []
                for submission in submission_details:
                    for attachment in submission.get("attachments", []):
                        attach_tasks.append(
                            download_submission_attachment_worker(client, submission, attachment, sem_attach))

                submission_details = [r for r in (await asyncio.gather(*submission_tasks)) if r]
                progress.update(task_overall, advance=1)

            sheets = prepare_data_for_export(forms, submission_details)

            await write_submissions_to_excel(sheets)
            await write_submissions_to_google_spreadsheet(sheets)
            progress.update(task_overall, advance=1)


if __name__ == "__main__":
    asyncio.run(main())
