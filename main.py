import os
import sqlite3
import httpx
import asyncio
from urllib.parse import urljoin
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress
from rich.progress import track
import json
from datetime import datetime
import xlsxwriter
from collections import defaultdict

load_dotenv()

console = Console()
error_console = Console(stderr=True, style="bold red")
os.makedirs(os.path.join('exported-data', os.getenv("ELECTION_ID")), exist_ok=True)
db_path = os.path.join('exported-data', os.getenv("DB_FILE"))
JWT = ""
download_attachments = os.getenv("DOWNLOAD_ATTACHMENTS", "").lower() == "true"

CREATE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS submissions
    (
        submissionId
        TEXT
        PRIMARY
        KEY,
        electionRoundId
        TEXT,
        timeSubmitted
        TEXT,
        formId
        TEXT,
        followUpStatus
        TEXT,
        pollingStationId
        TEXT,
        level1
        TEXT,
        level2
        TEXT,
        level3
        TEXT,
        level4
        TEXT,
        level5
        TEXT,
        number
        TEXT,
        monitoringObserverId
        TEXT,
        isOwnObserver
        BOOLEAN,
        observerName
        TEXT,
        email
        TEXT,
        phoneNumber
        TEXT,
        tags
        TEXT,
        ngoName
        TEXT,
        numberOfFlaggedAnswers
        INTEGER,
        numberOfQuestionsAnswered
        INTEGER,
        answers
        JSON
        NOT
        NULL
        DEFAULT
        '[]',
        arrivalTime
        TEXT,
        departureTime
        TEXT,
        breaks
        JSON
        NOT
        NULL
        DEFAULT
        '[]',
        isCompleted
        BOOLEAN
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS notes
    (
        id
        INTEGER
        PRIMARY
        KEY
        AUTOINCREMENT,
        submissionId
        TEXT,
        questionId
        TEXT,
        text
        TEXT,
        timeSubmitted
        TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS attachments
    (
        id
        INTEGER
        PRIMARY
        KEY
        AUTOINCREMENT,
        submissionId
        TEXT,
        questionId
        TEXT,
        path
        TEXT
    )
    """,
    """
    CREATE TABLE quick_reports
    (
        id                      TEXT PRIMARY KEY NOT NULL,
        electionRoundId         TEXT             NOT NULL,
        quickReportLocationType TEXT             NOT NULL,
        timestamp               TEXT             NOT NULL,              -- ISO8601 datetime string
        title                   TEXT             NOT NULL,
        description             TEXT,
        monitoringObserverId    TEXT             NOT NULL,
        isOwnObserver           INTEGER          NOT NULL,              -- 0 = false, 1 = true
        observerName            TEXT,
        email                   TEXT,
        phoneNumber             TEXT,
        tags                    JSON             NOT NULL DEFAULT '[]', -- JSON array
        pollingStationId        TEXT,
        level1                  TEXT,
        level2                  TEXT,
        level3                  TEXT,
        level4                  TEXT,
        level5                  TEXT,
        number                  TEXT,
        address                 TEXT,
        pollingStationDetails   TEXT,
        incidentCategory        TEXT,
        followUpStatus          TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quick_report_attachments
    (
        id
        INTEGER
        PRIMARY
        KEY
        AUTOINCREMENT,
        quickReportId
        TEXT,
        path
        TEXT
    )
    """,
    """
    CREATE TABLE forms
    (
        id                         TEXT PRIMARY KEY NOT NULL,
        electionRoundId            TEXT             NOT NULL,
        formType                   TEXT             NOT NULL,
        code                       TEXT             NOT NULL,
        name                       JSON             NOT NULL,
        status                     TEXT             NOT NULL,
        defaultLanguage            TEXT             NOT NULL,
        languages                  JSON             NOT NULL DEFAULT '[]', -- stored as JSON array
        questions                  JSON             NOT NULL DEFAULT '[]',
        description                JSON             NOT NULL DEFAULT '{}',
        numberOfQuestions          INTEGER,
        languagesTranslationStatus JSON             NOT NULL DEFAULT '{}'
    )
    """
]


def create_database():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    for sql in CREATE_TABLES_SQL:
        cursor.execute(sql)
    conn.commit()
    conn.close()
    console.log(f"Database '{db_path}' created with tables.")


def ensure_db():
    with console.status("[bold green]Setting up db ...") as status:
        if not os.path.exists(db_path):
            create_database()
        else:
            console.log(f"Database '{db_path}' already exists — skipping creation.")


async def log_in():
    global JWT
    with console.status("[bold green]Logging in...") as status:
        async with httpx.AsyncClient() as client:
            url = urljoin(os.getenv("BASE_API_URL"), "/api/auth/login")
            data = {"email": os.getenv("ADMIN_EMAIL"), "password": os.getenv("ADMIN_PASSWORD")}
            response = await client.post(url, json=data)

            response.raise_for_status()
            auth = response.json()
            JWT = auth.get("token")
            console.log('Logged in successfully!')


def get_local_submission_attachment_path(submission_id, attachment):
    attachment_path = os.path.abspath(os.path.join(
        'exported-data',
        os.getenv("ELECTION_ID"),
        f"submission-{submission_id}",
        attachment["uploadedFileName"]
    ))
    return attachment_path


def get_local_quick_report_attachment_path(quick_report_id, attachment):
    return os.path.abspath(os.path.join(
        'exported-data',
        os.getenv("ELECTION_ID"),
        f"quick-reports-{quick_report_id}",
        attachment["uploadedFileName"]
    ))


async def download_submission_attachment(submission, attachment):
    submission_id = submission['submissionId']
    path = os.path.join('exported-data', os.getenv("ELECTION_ID"), f"submission-{submission_id}")
    os.makedirs(path, exist_ok=True)
    attachment_path = get_local_submission_attachment_path(submission_id, attachment)

    if not os.path.exists(attachment_path):
        async with httpx.AsyncClient() as client:
            response = await client.get(attachment['presignedUrl'], timeout=None)
            response.raise_for_status()  # Raise error if request failed

            # Write content to file
            with open(attachment_path, "wb") as f:
                f.write(response.content)


async def download_quick_report_attachment(quick_report, attachment):
    quick_report_id = quick_report['id']
    path = os.path.join('exported-data', os.getenv("ELECTION_ID"), f"quick-reports-{quick_report_id}")
    os.makedirs(path, exist_ok=True)
    attachment_path = get_local_quick_report_attachment_path(quick_report_id, attachment)

    if not os.path.exists(attachment_path):
        async with httpx.AsyncClient() as client:
            response = await client.get(attachment['presignedUrl'], timeout=None)
            response.raise_for_status()  # Raise error if request failed

            # Write content to file
            with open(attachment_path, "wb") as f:
                f.write(response.content)


async def store_submission_data(submission):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Insert submission
    cursor.execute("""
    INSERT OR REPLACE INTO submissions VALUES (
        :submissionId,
        :electionRoundId,
        :timeSubmitted,
        :formId,
        :followUpStatus,
        :pollingStationId,
        :level1,
        :level2,
        :level3,
        :level4,
        :level5,
        :number,
        :monitoringObserverId,
        :isOwnObserver,
        :observerName,
        :email,
        :phoneNumber,
        :tags,
        :ngoName,
        :numberOfFlaggedAnswers,
        :numberOfQuestionsAnswered,
        :answers,
        :arrivalTime,
        :departureTime,
        :breaks,
        :isCompleted
    )
    """, {
        "submissionId": submission["submissionId"],
        "electionRoundId": os.getenv('ELECTION_ID'),
        "timeSubmitted": submission["timeSubmitted"],
        "formId": submission["formId"],
        "followUpStatus": submission["followUpStatus"],
        "pollingStationId": submission["pollingStationId"],
        "level1": submission["level1"],
        "level2": submission["level2"],
        "level3": submission["level3"],
        "level4": submission["level4"],
        "level5": submission["level5"],
        "number": submission["number"],
        "monitoringObserverId": submission["monitoringObserverId"],
        "isOwnObserver": int(submission["isOwnObserver"]),
        "observerName": submission["observerName"],
        "email": submission["email"],
        "phoneNumber": submission["phoneNumber"],
        "tags": json.dumps(submission["tags"]),
        "ngoName": submission["ngoName"],
        "numberOfFlaggedAnswers": submission["numberOfFlaggedAnswers"],
        "numberOfQuestionsAnswered": submission["numberOfQuestionsAnswered"],
        "answers": json.dumps(submission["answers"]),
        "arrivalTime": submission["arrivalTime"],
        "departureTime": submission["departureTime"],
        "breaks": json.dumps(submission["breaks"]),
        "isCompleted": int(submission["isCompleted"])
    })

    # Insert notes
    for note in submission.get("notes", []):
        cursor.execute("""
                       INSERT INTO notes (submissionId, questionId, text, timeSubmitted)
                       VALUES (?, ?, ?, ?)
                       """, (
                           submission["submissionId"],  # Link to the main submission
                           note["questionId"],
                           note["text"],
                           note["timeSubmitted"]
                       ))

    mapped_attachments = []
    # Insert attachments
    for attachment in submission.get("attachments", []):
        mapped_attachment = {
            "submissionId": submission["submissionId"],  # Link to the main submission
            "questionId": attachment["questionId"],
            "path": get_local_submission_attachment_path(submission["submissionId"], attachment)
            if download_attachments
            else attachment["presignedUrl"]
        }

        mapped_attachments.append(mapped_attachment)

        cursor.execute("""
                       INSERT INTO attachments (submissionId, questionId, path)
                       VALUES (:submissionId, :questionId, :path)
                       """, mapped_attachment)

    conn.commit()
    conn.close()

    return submission.update({"mapped_attachments": mapped_attachments})


async def store_quick_report_data(quick_report):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Insert submission
    cursor.execute("""
    INSERT OR REPLACE INTO quick_reports VALUES (
        :id,
        :electionRoundId,
        :quickReportLocationType,
        :timestamp,
        :title,
        :description,
        :monitoringObserverId,
        :isOwnObserver,
        :observerName,
        :email,
        :phoneNumber,
        :tags,
        :pollingStationId,
        :level1,
        :level2,
        :level3,
        :level4,
        :level5,
        :number,
        :address,
        :pollingStationDetails,
        :incidentCategory,
        :followUpStatus
    )
    """, {
        "id": quick_report["id"],
        "electionRoundId": os.getenv('ELECTION_ID'),
        "quickReportLocationType": quick_report["quickReportLocationType"],
        "timestamp": quick_report["timestamp"],
        "title": quick_report["title"],
        "description": quick_report["description"],
        "monitoringObserverId": quick_report["monitoringObserverId"],
        "isOwnObserver": int(quick_report["isOwnObserver"]),
        "observerName": quick_report["observerName"],
        "email": quick_report["email"],
        "phoneNumber": quick_report["phoneNumber"],
        "tags": json.dumps(quick_report["tags"]),
        "pollingStationId": quick_report["pollingStationId"],
        "level1": quick_report["level1"],
        "level2": quick_report["level2"],
        "level3": quick_report["level3"],
        "level4": quick_report["level4"],
        "level5": quick_report["level5"],
        "number": quick_report["number"],
        "address": quick_report["address"],
        "pollingStationDetails": quick_report["pollingStationDetails"],
        "incidentCategory": quick_report["incidentCategory"],
        "followUpStatus": quick_report["followUpStatus"],
    })

    mapped_attachments = []
    # Insert attachments
    for attachment in quick_report.get("attachments", []):
        mapped_attachment = {
            "quickReportId": quick_report["id"],  # Link to the main submission
            "path": get_local_quick_report_attachment_path(quick_report["id"], attachment)
            if download_attachments
            else attachment["presignedUrl"]
        }

        mapped_attachments.append(mapped_attachment)

        cursor.execute("""
                       INSERT INTO quick_report_attachments (quickReportId, path)
                       VALUES (:quickReportId, :path)
                       """, mapped_attachment)
    conn.commit()
    conn.close()
    return quick_report.update({"mapped_attachments": mapped_attachments})


async def download_submission_data(submission):
    submission_id = submission['submissionId']
    async with httpx.AsyncClient() as client:
        url = urljoin(
            os.getenv("BASE_API_URL"),
            f"/api/election-rounds/{os.getenv('ELECTION_ID')}/form-submissions/{submission_id}:v2"
        )
        headers = {"Authorization": f"Bearer {JWT}"}

        response = await client.get(url, headers=headers)
        response.raise_for_status()
        submission_data = response.json()

        await store_submission_data(submission_data)

        attachments = submission_data.get("attachments", [])
        if len(attachments) > 0 and download_attachments:
            for attachment in track(attachments,
                                    description=f"[cyan]Downloading attachments for submission {submission_id}..."):
                await download_submission_attachment(submission, attachment)

        return submission_data


async def download_form(form_id):
    async with httpx.AsyncClient() as client:
        url = urljoin(
            os.getenv("BASE_API_URL"),
            f"/api/election-rounds/{os.getenv('ELECTION_ID')}/forms/{form_id}"
        )
        headers = {"Authorization": f"Bearer {JWT}"}

        response = await client.get(url, headers=headers)
        response.raise_for_status()
        form = response.json()

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Insert submission
        cursor.execute("""
           INSERT OR REPLACE INTO forms VALUES (
                :id,
                :electionRoundId,
                :formType,
                :code,
                :name,
                :status,
                :defaultLanguage,
                :languages,
                :questions,
                :description,
                :numberOfQuestions,
                :languagesTranslationStatus
           )
           """, {
            "id": form["id"],
            "electionRoundId": os.getenv('ELECTION_ID'),
            "formType": form["formType"],
            "code": form["code"],
            "name": json.dumps(form["name"]),
            "status": form["status"],
            "defaultLanguage": form["defaultLanguage"],
            "languages": json.dumps(form["languages"]),
            "questions": json.dumps(form["questions"]),
            "description": json.dumps(form["description"]),
            "numberOfQuestions": form["numberOfQuestions"],
            "languagesTranslationStatus": json.dumps(form["languagesTranslationStatus"])
        })

        conn.commit()
        conn.close()
        return form


def build_answers(questions_options_dict, question, answers, attachments_by_question, notes_by_question,
                  default_language):
    question_id = question["id"]
    answer = next((a for a in answers if a["questionId"] == question_id), {})
    notes = "\n".join(notes_by_question.get(question_id, []))
    attachments = "\n".join(attachments_by_question.get(question_id, []))
    has_free_text_option = any(
        opt.get("isFreeText")
        for opt in questions_options_dict.get(question_id, [])
    )

    match question["$questionType"]:
        case "textQuestion":
            if not answer or not answer.get("text"):
                return ["", notes, attachments]
            else:
                return [
                    answer["text"],
                    notes,
                    attachments,
                ]
        case "numberQuestion":
            if not answer or not answer.get("value"):
                return ["", notes, attachments]
            else:
                return [
                    answer["value"],
                    notes,
                    attachments,
                ]
        case "dateQuestion":
            if not answer or not answer.get("date"):
                return ["", notes, attachments]
            else:
                date = datetime.fromisoformat(answer["date"].replace("Z", "+00:00"))
                return [
                    date.strftime("%Y-%m-%d %H:%M:%S"),
                    notes,
                    attachments,
                ]
        case "singleSelectQuestion":
            if not answer or not answer.get("selection"):
                if has_free_text_option:
                    return ["", "", notes, attachments]
                else:
                    return ["", notes, attachments]
            else:
                option = next(
                    (o for o in questions_options_dict[question_id]
                     if o["id"] == answer["selection"]["optionId"]),
                    None
                )

                selection = option["text"][default_language] if option else ""
                if has_free_text_option:
                    return [
                        selection,
                        answer["selection"].get("text", ""),
                        notes,
                        attachments,
                    ]
                else:
                    return [
                        selection,
                        notes,
                        attachments,
                    ]

        case "multiSelectQuestion":
            if not answer or not answer.get("selection"):
                if has_free_text_option:
                    return ["", "", notes, attachments]
                else:
                    return ["", notes, attachments]

            else:
                selection = ", ".join([
                    next(
                        (o for o in questions_options_dict[question_id]
                         if o["id"] == sel["optionId"]),
                        None
                    )["text"][default_language]
                    for sel in answer["selection"]
                    if sel.get("optionId") and any(
                        o["id"] == sel["optionId"] for o in questions_options_dict[question_id])
                ])
                if has_free_text_option:
                    return [
                        selection,
                        ", ".join(sel.get("text", "") for sel in answer["selection"] if sel.get("text")),
                        notes,
                        attachments,
                    ]
                else:
                    return [
                        selection,
                        notes,
                        attachments,
                    ]
        case "ratingQuestion":
            if not answer or not answer.get("value"):
                return ["", notes, attachments]
            else:
                return [
                    answer["value"],
                    notes,
                    attachments,
                ]
        case _:
            return ["unknown value", notes, attachments]


async def export_form_submissions(forms, submissions):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"submissions_{timestamp}.xlsx"
    path = os.path.join('exported-data', os.getenv("ELECTION_ID"), filename)

    workbook = xlsxwriter.Workbook(path)

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

    for idx, form in enumerate(forms, start=1):
        sheet_name = (
            f"{idx}_PSI" if form["formType"] == 'PSI'
            else f"{idx}_{form['name'][form['defaultLanguage']][:31]}"
        )

        form_worksheet = workbook.add_worksheet(sheet_name)
        # Build headers
        form_headers = list(default_headers)  # start with defaults

        for question in form["questions"]:
            # base column: question text
            form_headers.append(f"{question["code"]} - {question["text"][form["defaultLanguage"]]}")

            # optional FreeText column
            if (
                    question.get("$questionType") in ("singleSelectQuestion", "multiSelectQuestion")
                    and any(opt.get("isFreeText") for opt in question.get("options", []))
            ):
                form_headers.append("FreeText", )

            # always add Notes and Attachments
            form_headers.append("Notes")
            form_headers.append("Attachments")

        # Filter only select-type questions and build a dict of id -> options
        questions_options_dict = {
            q["id"]: q["options"]
            for q in form["questions"]
            if q["$questionType"] in ("singleSelectQuestion", "multiSelectQuestion")
        }

        # Filter submissions for this form
        form_submissions = [
            sub for sub in submissions
            if sub["formId"] == form["id"]
        ]

        # Build all rows
        rows = []
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

            for attachment in fs.get("mapped_attachments", []):
                attachments_by_question[attachment["questionId"]].append(attachment["path"])

            notes_by_question = defaultdict(list)

            for note in fs.get("notes", []):
                notes_by_question[note["questionId"]].append(note["text"])

            # Append each question's answers
            for question in form["questions"]:
                answers = fs.get("answers", [])
                row = build_answers(questions_options_dict, question, answers, attachments_by_question,
                                    notes_by_question,
                                    form["defaultLanguage"])
                row_data.extend(row)

            rows.append(row_data)

        # --- Write headers ---
        for col, header in enumerate(form_headers):
            form_worksheet.write_string(0, col, header)

        # --- Write data rows ---
        for row_idx, row_data in enumerate(rows, start=1):
            for col_idx, cell_value in enumerate(row_data):
                form_worksheet.writewrite_string(row_idx, col_idx, cell_value or "")

    workbook.close()


async def download_form_submissions():
    all_submissions = []
    page_number = 1
    page_size = 100
    headers = {"Authorization": f"Bearer {JWT}"}
    url = urljoin(
        os.getenv("BASE_API_URL"),
        f"/api/election-rounds/{os.getenv('ELECTION_ID')}/form-submissions:byEntry"
    )

    with Progress(console=console) as progress:
        list_fs_task = progress.add_task("[cyan]Downloading submissions...", start=False)

        async with httpx.AsyncClient() as client:
            while True:  # simulate do-while
                params = {
                    "dataSource": "Coalition",
                    "pageNumber": page_number,
                    "pageSize": page_size
                }

                response = await client.get(url, params=params, headers=headers)
                response.raise_for_status()
                data = response.json()

                items = data.get("items", [])
                total_count = data.get("totalCount", 0)

                if not progress.tasks[list_fs_task].started:
                    progress.update(list_fs_task, total=total_count)
                    progress.start_task(list_fs_task)

                all_submissions.extend(items)
                progress.update(list_fs_task, advance=len(items))

                if len(items) < page_size:
                    break

                page_number += 1

    all_submissions_data = []
    for submission in track(all_submissions, description="[cyan]Downloading submissions data..."):
        all_submissions_data.append(await download_submission_data(submission))

    all_forms = []
    form_ids = set(map(lambda x: x['formId'], all_submissions))
    for form_id in track(form_ids, description=f"[cyan]Downloading forms ..."):
        all_forms.append(await download_form(form_id))

    await export_form_submissions(all_forms, all_submissions_data)
    console.log(f"✅ Downloaded {len(all_submissions)} submissions total.")


async def download_quick_report_data(quick_report):
    quick_report_id = quick_report['id']
    async with httpx.AsyncClient() as client:
        url = urljoin(
            os.getenv("BASE_API_URL"),
            f"/api/election-rounds/{os.getenv('ELECTION_ID')}/quick-reports/{quick_report_id}"
        )
        headers = {"Authorization": f"Bearer {JWT}"}

        response = await client.get(url, headers=headers)
        response.raise_for_status()
        quick_report_data = response.json()

        await store_quick_report_data(quick_report_data)

        attachments = quick_report_data.get("attachments", [])
        if len(attachments) > 0 and download_attachments:
            for attachment in track(attachments,
                                    description=f"[cyan]Downloading attachments for quick report {quick_report_id}..."):
                await download_quick_report_attachment(quick_report, attachment)

        return quick_report_data


async def export_quick_reports(quick_reports):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"quick-reports_{timestamp}.xlsx"
    path = os.path.join('exported-data', os.getenv("ELECTION_ID"), filename)

    workbook = xlsxwriter.Workbook(path)
    worksheet = workbook.add_worksheet("quick-reports")

    headers = [
        "QuickReportId",
        "TimeSubmitted",
        "FollowUpStatus",
        "IncidentCategory",
        "Ngo",
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
    ]

    # --- Write headers ---
    for col, header in enumerate(headers):
        worksheet.write(0, col, header)

    # Build all rows
    rows = []
    for qr in quick_reports:
        attachments = "\n\n".join(map(lambda a: a["path"], qr.get("mapped_attachments", [])))
        row_data = [
            qr.get("quickReportId", ""),
            qr.get("timeSubmitted", ""),
            qr.get("followUpStatus", ""),
            qr.get("incidentCategory", ""),
            qr.get("ngo", ""),
            qr.get("monitoringObserverId", ""),
            qr.get("name", ""),
            qr.get("email", ""),
            qr.get("phoneNumber", ""),
            qr.get("locationType", ""),
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
        rows.append(row_data)

    # --- Write data rows ---
    for row_idx, row_data in enumerate(rows, start=1):
        for col_idx, cell_value in enumerate(row_data):
            worksheet.write_string(row_idx, col_idx, cell_value or "")

    workbook.close()


async def download_quick_reports():
    all_quick_reports = []
    page_number = 1
    page_size = 100
    headers = {"Authorization": f"Bearer {JWT}"}
    url = urljoin(
        os.getenv("BASE_API_URL"),
        f"/api/election-rounds/{os.getenv('ELECTION_ID')}/quick-reports"
    )

    with Progress(console=console) as progress:
        list_fs_task = progress.add_task("[cyan]Downloading quick reports...", start=False)

        async with httpx.AsyncClient() as client:
            while True:  # simulate do-while
                params = {
                    "dataSource": "Coalition",
                    "pageNumber": page_number,
                    "pageSize": page_size
                }

                response = await client.get(url, params=params, headers=headers)
                response.raise_for_status()
                data = response.json()

                items = data.get("items", [])
                total_count = data.get("totalCount", 0)

                if not progress.tasks[list_fs_task].started:
                    progress.update(list_fs_task, total=total_count)
                    progress.start_task(list_fs_task)

                all_quick_reports.extend(items)
                progress.update(list_fs_task, advance=len(items))

                if len(items) < page_size:
                    break

                page_number += 1

    all_quick_reports_data = []
    for quick_report in track(all_quick_reports, description="[cyan]Downloading quick reports data..."):
        all_quick_reports_data.append(await download_quick_report_data(quick_report))

    await export_quick_reports(all_quick_reports_data)

    console.log(f"✅ Downloaded {len(all_quick_reports)} quick reports total.")


async def main():
    ensure_db()
    await log_in()
    await download_form_submissions()
    await download_quick_reports()


if __name__ == "__main__":
    asyncio.run(main())
