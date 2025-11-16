import asyncio
import os
from collections import defaultdict
import re
from typing import Any, Dict, Optional, List
from urllib.parse import urljoin

import httpx
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress
import xlsxwriter
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from textual.widgets import data_table
from zoneinfo import ZoneInfo

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
FS_GOOGLE_DOC_ID = os.getenv("FS_GOOGLE_DOC_ID", "")
QR_GOOGLE_DOC_ID = os.getenv("QR_GOOGLE_DOC_ID", "")
scopes = ['https://www.googleapis.com/auth/spreadsheets']
credentials = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_PATH, scopes=scopes)
gdocs_client = gspread.authorize(credentials)

ZONE_INFO = ZoneInfo(os.getenv("ZONE_INFO", "Etc/UTC"))

# ----------------------------------------------------------------------------------

# ---------------------------
# Utilities
# ---------------------------
def clean_sheet_name(name: str) -> str:
    # Remove invalid characters
    name = re.sub(r'[\[\]\:\*\?\/\\]', '', name)
    # Trim to max length (31)
    return name[:31].strip()


def local_submission_attachment_path(attachment: Dict[str, Any]) -> str:
    return os.path.abspath(os.path.join(EXPORT_ROOT, 'submission-attachments', attachment["uploadedFileName"]))


def local_quick_report_attachment_path(attachment: Dict[str, Any]) -> str:
    return os.path.abspath(
        os.path.join(EXPORT_ROOT, f"quick-report-attachments", attachment["uploadedFileName"]))


# ---------------------------
# Fetch and download helpers
# ---------------------------
async def fetch_json(client: httpx.AsyncClient, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


async def download_binary(url: str, target_path):
    if os.path.exists(target_path):
        return

    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    if not os.path.exists(target_path):
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=None)
            response.raise_for_status()  # Raise error if request failed

            # Write content to file
            with open(target_path, "wb") as f:
                f.write(response.content)


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
    url = urljoin(BASE_API_URL, f"/api/election-rounds/{ELECTION_ID}/forms/{form_id}")
    form = await fetch_json(client, url)
    progress.update(task, advance=1)
    return form

async def fetch_all_forms(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    url = urljoin(BASE_API_URL, f"/api/election-rounds/{ELECTION_ID}/forms")
    params = {"pageNumber": 1, "pageSize": 100, "dataSource": "Coalition"}
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    forms = [item for item in data.get("items", []) if item.get("status") != "Drafted"]
    

    # temp workaround to fetch form details for PSI
    forms.append({"id":'d4a0c5ca-4dbd-47c0-8854-ba8cb2adbe10'})

    return forms


async def fetch_quick_report_detail(client: httpx.AsyncClient, quick_report_id: str, sem: asyncio.Semaphore, progress,
                                    task) -> Dict[str, Any]:
    url = urljoin(BASE_API_URL, f"/api/election-rounds/{ELECTION_ID}/quick-reports/{quick_report_id}")
    async with sem:
        try:
            quick_report = await fetch_json(client, url)
            progress.update(task, advance=1)
            return quick_report
        except Exception as e:
            error_console.log(f"Failed to download submission {quick_report_id}: {e}")


async def fetch_all_form_submissions(client: httpx.AsyncClient) -> List[
    Dict[str, Any]]:
    url = urljoin(BASE_API_URL, f"/api/election-rounds/{ELECTION_ID}/form-submissions:byEntry")

    page_number = 1
    page_size = 100
    form_submissions: List[Dict[str, Any]] = []
    while True:
        params = {"pageNumber": page_number, "pageSize": page_size, "dataSource": "Coalition"}
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        form_submissions.extend(items)
        if len(items) < page_size:
            break
        page_number += 1
    return form_submissions


async def fetch_all_quick_reports(client: httpx.AsyncClient) -> List[
    Dict[str, Any]]:
    url = urljoin(BASE_API_URL, f"/api/election-rounds/{ELECTION_ID}/quick-reports")

    page_number = 1
    page_size = 100
    quick_reports: List[Dict[str, Any]] = []
    while True:
        params = {"pageNumber": page_number, "pageSize": page_size, "dataSource": "Coalition"}
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        quick_reports.extend(items)
        if len(items) < page_size:
            break
        page_number += 1
    return quick_reports


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

def map_submission_follow_up_status(follow_up_status):
    status_map = {
        "NotApplicable": "Not Applicable",
        "NeedsFollowUp": "Needs Follow-up",
        "Resolved": "Resolved",
    }

    return status_map.get(follow_up_status, follow_up_status)

def map_quick_report_incident_category(incident_category):
    incident_category_map = {
      "PhysicalViolenceIntimidationPressure": "Physical violence/intimidation/pressure",
      "CampaigningAtPollingStation": "Campaigning at the polling station",
      "RestrictionOfObserversRights": "Restriction of observer's (representative's/media) rights",
      "UnauthorizedPersonsAtPollingStation": "Unauthorized person(s) at the polling station",
      "ViolationDuringVoterVerificationProcess": "Violation during voter verification process",
      "VotingWithImproperDocumentation": "Voting with improper documentation",
      "IllegalRestrictionOfVotersRightToVote": "Illegal restriction of voter's right to vote",
      "DamagingOrSeizingElectionMaterials": "Damaging of/seizing election materials",
      "ImproperFilingOrHandlingOfElectionDocumentation": "Improper filing/handling of election documentation",
      "BallotStuffing": "Ballot stuffing",
      "ViolationsRelatedToControlPaper": "Violations related to the control paper",
      "NotCheckingVoterIdentificationSafeguardMeasures": "Not checking the voter identification safeguard measures",
      "VotingWithoutVoterIdentificationSafeguardMeasures": "Voting without voter identification safeguard measures",
      "BreachOfSecrecyOfVote": "Breach of secrecy of vote",
      "ViolationsRelatedToMobileBallotBox": "Violations related to the mobile ballot box",
      "NumberOfBallotsExceedsNumberOfVoters": "Number of ballots exceed the number of voters",
      "ImproperInvalidationOrValidationOfBallots": "Improper invalidation / validation of ballots",
      "FalsificationOrImproperCorrectionOfFinalProtocol": "Falsification / improper correction of the final protocol",
      "RefusalToIssueCopyOfFinalProtocolOrIssuingImproperCopy": "Refusal to issue a copy of the final protocol/issuing an improper copy",
      "ImproperFillingInOfFinalProtocol": "Improper filling in of the final protocol",
      "ViolationOfSealingProceduresOfElectionMaterials": "Violation of the sealing procedures of the election materials",
      "ViolationsRelatedToVoterLists": "Violations related to the voter lists",
      "Other": "Other"
    }

    return incident_category_map.get(incident_category, incident_category)

def map_quick_report_location_type(location_type):
    location_type_map = {
      "NotRelatedToAPollingStation": "Not Related To A Polling Station",
      "OtherPollingStation": "Other Polling Station",
      "VisitedPollingStation": "Visited Polling Station"
    }

    return location_type_map.get(location_type, location_type)

def submissions_to_data_table(forms: List[Dict[str, Any]], submissions: List[Dict[str, Any]]):
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
        form_submissions.sort(key=lambda x: x.get("timeSubmitted", ""))

        data = [form_headers]
        for fs in form_submissions:
            timeSubmitted_utc = datetime.fromisoformat(fs.get("timeSubmitted", "").replace("Z", "+00:00"))

            # Convert from UTC to specified timezone
            timeSubmitted = timeSubmitted_utc.astimezone(ZONE_INFO).strftime("%Y-%m-%d %H:%M:%S")
            row_data = [
                fs.get("submissionId", ""),
                timeSubmitted,
                map_submission_follow_up_status(fs.get("followUpStatus", "")),
                fs.get("level1", ""),
                fs.get("level2", ""),
                fs.get("level3", ""),
                fs.get("level4", ""),
                fs.get("level5", ""),
                fs.get("number", ""),
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


def quick_reports_to_data_table(quick_reports):
    data_table = [[
        "QuickReportId",
        "TimeSubmitted",
        "FollowUpStatus",
        "IncidentCategory",
        "MonitoringObserverId",
        "Name",
        "Email",
        "PhoneNumber",
        "LocationType",
        "Level1",
        "Level2",
        "Level3",
        "Level4",
        "Level5",
        "LevelNumber",
        "PollingStationDetails",
        "Title",
        "Description",
        "Attachments",
    ]]
    quick_reports.sort(key=lambda x: x.get("timestamp", ""))

    for qr in quick_reports:
        attachments = "\n\n".join(map(lambda a: a["presignedUrl"], qr.get("attachments", [])))
        timeSubmitted_utc = datetime.fromisoformat(qr.get("timestamp", "").replace("Z", "+00:00"))

        # Convert from UTC to specified timezone
        timeSubmitted = timeSubmitted_utc.astimezone(ZONE_INFO).strftime("%Y-%m-%d %H:%M:%S")
        row_data = [
            qr.get("id", ""),
            timeSubmitted,
            map_submission_follow_up_status(qr.get("followUpStatus", "")),
            map_quick_report_incident_category(qr.get("incidentCategory", "")),
            qr.get("monitoringObserverId", ""),
            qr.get("name", ""),
            qr.get("email", ""),
            qr.get("phoneNumber", ""),
            map_quick_report_location_type(qr.get("quickReportLocationType", "")),
            qr.get("level1", ""),
            qr.get("level2", ""),
            qr.get("level3", ""),
            qr.get("level4", ""),
            qr.get("level5", ""),
            qr.get("levelNumber", ""),
            qr.get("pollingStationDetails", ""),
            qr.get("title", ""),
            qr.get("description", ""),
            attachments
        ]
        data_table.append(row_data)

    return data_table


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


async def write_quick_reports_to_excel(quick_reports):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    filename = f"quick-reports-{timestamp}.xlsx"
    path = os.path.join(EXPORT_ROOT, filename)

    workbook = xlsxwriter.Workbook(path)

    quick_reports_worksheet = workbook.add_worksheet("Quick Reports")

    # write data
    for row_idx, row_data in enumerate(quick_reports):
        for col_idx, cell_value in enumerate(row_data):
            quick_reports_worksheet.write_string(row_idx, col_idx, str(cell_value or ""))

    workbook.close()


async def write_submissions_to_google_spreadsheet(progress, task_upload_forms, forms_data):
    workbook = gdocs_client.open_by_key(FS_GOOGLE_DOC_ID)

    for sheet_name, data in forms_data.items():
        # Try to get existing worksheet or create a new one
        try:
            worksheet = workbook.worksheet(sheet_name)
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            num_rows = max(1000, len(data))
            num_cols = max(1, len(data[0]) if data else 1)
            worksheet = workbook.add_worksheet(title=sheet_name, rows=num_rows, cols=num_cols)

        # Convert all cells to strings
        values = [[str(cell or "") for cell in row] for row in data]

        worksheet.update(range_name="A1", values=values)
        progress.update(task_upload_forms, advance=1)

async def write_quick_reports_to_google_spreadsheet(quick_reports):
    workbook = gdocs_client.open_by_key(QR_GOOGLE_DOC_ID)
    sheet_name = "Quick Reports"

    # Try to get existing worksheet or create a new one
    try:
        worksheet = workbook.worksheet(sheet_name)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        num_rows = max(1000, len(quick_reports))
        worksheet = workbook.add_worksheet(title=sheet_name, rows=num_rows, cols=100)

    # Convert all cells to strings
    values = [[str(cell or "") for cell in row] for row in quick_reports]

    worksheet.update(range_name="A1", values=values)

async def write_timestamp_to_google_spreadsheet(workbook, timestamp):
    sheet_name = "Status"

    # Try to get existing worksheet or create a new one
    try:
        worksheet = workbook.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = workbook.add_worksheet(title=sheet_name, rows=1000, cols=10)

    worksheet.update_acell("B3", timestamp)

async def download_submission_attachment_worker(attachment: Dict[str, Any], sem: asyncio.Semaphore, progress, task) -> None:
    path = local_submission_attachment_path(attachment)
    async with sem:
        try:
            await download_binary(attachment["presignedUrl"], path)
            progress.update(task, advance=1)
        except Exception as e:
            error_console.log(f"{e}")

async def download_quick_report_attachment_worker(attachment: Dict[str, Any], sem: asyncio.Semaphore, progress, task) -> None:
    path = local_quick_report_attachment_path(attachment)
    async with sem:
        try:
            await download_binary(attachment["presignedUrl"], path)
            progress.update(task, advance=1)
        except Exception as e:
            error_console.log(f"{e}")

async def main():
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS) as client:
        await log_in(client)

        with Progress(console=console, transient=True) as progress:
            task_overall = progress.add_task("[cyan]Overall progress...", total=14)

            # 1 Fetch submission list
            submissions_list = await fetch_all_form_submissions(client)
            progress.update(task_overall, advance=1)

            # 2 Fetch submission details concurrently
            sem_submissions = asyncio.Semaphore(CONCURRENT_WORKERS)

            task_submissions = progress.add_task("[green]Fetching submissions...", total=len(submissions_list))

            form_submissions_tasks = [
                fetch_submission_detail(client, s["submissionId"], sem_submissions, progress, task_submissions)
                for s in submissions_list
            ]
            form_submissions = [r for r in (await asyncio.gather(*form_submissions_tasks)) if r]
            progress.update(task_overall, advance=1)

            # 3 Fetch distinct forms
            forms = await fetch_all_forms(client)

            task_forms = progress.add_task("[green]Fetching form details...", total=len(forms))

            form_tasks = [fetch_form_detail(client, form.get("id"), progress, task_forms) for form in forms]
            forms = [f for f in (await asyncio.gather(*form_tasks)) if f]
            progress.update(task_overall, advance=1)

            # 4 Download forms submissions attachments
            if DOWNLOAD_ATTACHMENTS:
                sem_attach = asyncio.Semaphore(CONCURRENT_WORKERS)
                attach_tasks = []
                total_number_of_attachments = sum(
                    len(submission.get("attachments", []))
                    for submission in form_submissions
                )

                fs_attachments_task = progress.add_task("[green]Fetching form submissions attachments...", total=total_number_of_attachments)

                for submission in form_submissions:
                    for attachment in submission.get("attachments", []):
                        attach_tasks.append(
                            download_submission_attachment_worker(attachment, sem_attach, progress, fs_attachments_task))

                _ = [r for r in (await asyncio.gather(*attach_tasks)) if r]
            progress.update(task_overall, advance=1)

            # 5 Form submissions to spreadsheets
            submissions_sheets = submissions_to_data_table(forms, form_submissions)
            progress.update(task_overall, advance=1)

            # 6 Write form submissions to excel
            await write_submissions_to_excel(submissions_sheets)
            progress.update(task_overall, advance=1)

            # 7 Write form submissions to Google Spreadsheets
            task_upload_forms = progress.add_task("[green]Write form submissions to Google Spreadsheets...", total=len(forms))
            await write_submissions_to_google_spreadsheet(progress, task_upload_forms, submissions_sheets)
            progress.update(task_overall, advance=1)

            # 8 Fetch quick reports list
            quick_reports_list = await fetch_all_quick_reports(client)
            progress.update(task_overall, advance=1)

            # 9 Fetch quick reports details concurrently
            sem_quick_reports = asyncio.Semaphore(CONCURRENT_WORKERS)

            task_quick_reports = progress.add_task("[green]Fetching quick reports...", total=len(quick_reports_list))

            quick_reports_tasks = [
                fetch_quick_report_detail(client, qr["id"], sem_quick_reports, progress, task_quick_reports)
                for qr in quick_reports_list
            ]
            quick_reports_details = [r for r in (await asyncio.gather(*quick_reports_tasks)) if r]
            progress.update(task_overall, advance=1)

            # 10 Download quick reports attachments
            if DOWNLOAD_ATTACHMENTS:
                sem_attach = asyncio.Semaphore(CONCURRENT_WORKERS)
                attach_tasks = []
                total_number_of_attachments = sum(
                    len(qr.get("attachments", []))
                    for qr in quick_reports_details
                )

                qr_attachments_task = progress.add_task("[green]Fetching quick reports attachments...",
                                                        total=total_number_of_attachments)

                for quick_report in quick_reports_details:
                    for attachment in quick_report.get("attachments", []):
                        attach_tasks.append(
                            download_quick_report_attachment_worker(attachment, sem_attach, progress, qr_attachments_task))

                _ = [r for r in (await asyncio.gather(*attach_tasks)) if r]

            progress.update(task_overall, advance=1)

            # 11 Quick reports to spreadsheets
            quick_reports_sheet = quick_reports_to_data_table(quick_reports_details)
            progress.update(task_overall, advance=1)

            # 12 Write quick reports to excel
            await write_quick_reports_to_excel(quick_reports_sheet)
            progress.update(task_overall, advance=1)

            # 13 Write quick reports to Google Spreadsheets
            await write_quick_reports_to_google_spreadsheet(quick_reports_sheet)
            progress.update(task_overall, advance=1)

            fs_workbook = gdocs_client.open_by_key(FS_GOOGLE_DOC_ID)
            qr_workbook = gdocs_client.open_by_key(QR_GOOGLE_DOC_ID)

            timestamp = datetime.now(ZONE_INFO).strftime("%Y-%m-%d %H:%M:%S")
            await write_timestamp_to_google_spreadsheet(fs_workbook, timestamp)
            await write_timestamp_to_google_spreadsheet(qr_workbook, timestamp)
            progress.update(task_overall, advance=1)


if __name__ == "__main__":
    asyncio.run(main())
