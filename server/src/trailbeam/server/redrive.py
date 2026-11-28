"""Redrive (QV-590): moving the messages in a dead-letter queue back to the queues they came from, with SQS's own
message move task. This is the hosted app's redrive.ts (trailbeam-app lib/app/redrive.ts) on self-hosted Trailbeam's
one set of AWS credentials, boto3's own (aws_reader.py), instead of a redrive role per connected account. Trailbeam
calls only the three move-task actions:

    sqs:StartMessageMoveTask    start moving every message in the dead-letter queue back
    sqs:ListMessageMoveTasks    the status of the latest moves
    sqs:CancelMessageMoveTask   stop a move that is running

and reads the dead-letter queue's depth with sqs:GetQueueAttributes, which AWS reads already need. SQS's move task
then works with the same credentials' rights: it receives and deletes on the dead-letter queue and sends to each
message's source queue (REDRIVE_ON_DLQ, REDRIVE_ON_SOURCE).

SQS moves a whole dead-letter queue, oldest message first. It cannot move one message, so "Redrive" always names the
queue it empties. Each message moved gets a new message ID.

**Available only when AWS is on.** A dead-letter queue can be redriven when AWS reads are on (aws_settings.is_on),
boto3 is installed, the settings' ``redrive`` list names the queue (empty by default: redrive off), and the person
is an admin (anyone, before accounts exist, as for the settings). ``gate`` says which, with the hosted app's codes
where they mean the same thing, so one sentence explains a button that is off.

**Where it redrives (QV-740).** As for reads (aws_reader.py), a dead-letter queue's account and region come from
spans, which anyone who can send them chooses, so only the settings decide where SQS is called: the settings'
``accountId``, else the credentials' own (``place``, after sts:GetCallerIdentity), and the home region or one of the
other ``regions``. A queue whose spans name another account or region is refused, and its ARN and URL are built
from those, never taken from spans.

What a move's status means in words (moveWords, counted, the "redriven" notice on a message) is @trailbeam/ui's
(ui/src/redrive/words.ts), shared by both products; only ``next_check``, which this server needs to know when to read
a move again, is here, with the same constants.

The functions take an SQS client (boto3's, or a test's fake with the same methods), and never raise for what AWS
answers: each failure is a sentence.
"""
from __future__ import annotations

import re
from typing import Any, Optional

from . import aws_figures as af, aws_reader, aws_reads, aws_settings

#: what Trailbeam calls to redrive
REDRIVE_CALLS = ("sqs:StartMessageMoveTask", "sqs:ListMessageMoveTasks", "sqs:CancelMessageMoveTask")
#: what SQS's move task needs on the dead-letter queue, besides REDRIVE_CALLS (iam.ts REDRIVE_ON_DLQ)
REDRIVE_ON_DLQ = REDRIVE_CALLS + ("sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes")
#: what it needs on the queue a message goes back to
REDRIVE_ON_SOURCE = ("sqs:SendMessage",)

# ── the move's state over time (@trailbeam/ui redrive/words.ts, the same numbers) ─────────────────────────────────
#: Measured on real SQS (QV-60 §2): ListMessageMoveTasks can say COMPLETED for a move that has not moved everything
#: yet, so a finished move is believed only once the dead-letter queue's depth is read again SETTLE_MS later.
SETTLE_MS = 60_000
#: a finished move older than this, never checked, is not checked any more: its queue has moved on
CHECK_WITHIN_MS = 15 * 60_000
FINAL = ("COMPLETED", "CANCELLED", "FAILED")


def is_final(status: Optional[str]) -> bool:
    return status is not None and status in FINAL


def next_check(row: dict, now_ms: float) -> str:
    """What the next status read of a started move does: "task" (read the task), "depth" (read the dead-letter
    queue's depth), "wait" or "done". ``row``: a log entry (redrive_log.py) with status, statusAt, leftAt."""
    if not is_final(row.get("status")):
        return "task"
    if row.get("leftAt") is not None or row.get("statusAt") is None or now_ms - row["statusAt"] > CHECK_WITHIN_MS:
        return "done"
    return "depth" if now_ms - row["statusAt"] >= SETTLE_MS else "wait"


def counted(moved: Optional[int], to_move: Optional[int]) -> Optional[str]:
    """ "2 of 4 moved", in SQS's words (words.ts counted)."""
    if moved is None:
        return None
    if to_move is None:
        return f"{moved} moved"
    return "SQS counted 0 messages to move" if to_move == 0 and moved == 0 else f"{moved} of {to_move} moved"


# ── where a dead-letter queue is, and whether it may be redriven ─────────────────────────────────────────────────

def dlq_arn(account: str, region: str, name: str) -> str:
    partition = "aws-cn" if region.startswith("cn-") else "aws-us-gov" if region.startswith("us-gov-") else "aws"
    return f"arn:{partition}:sqs:{region}:{account}:{name}"


def dlq_url(account: str, region: str, name: str) -> str:
    return aws_reader.queue_url(name, region, account)


def name_of(arn: str) -> str:
    return arn.split(":")[-1] if arn else arn


OFF = "Redrive needs AWS turned on. An admin turns on AWS in Settings, under AWS, and names the dead-letter queues " \
      "Trailbeam may redrive."
NO_BOTO3 = 'Redrive needs boto3, which this server does not have. Install it with pip install "trailbeam[aws]".'
NOT_MANAGER = "Only an admin can redrive."


def not_named(name: str) -> str:
    return (f"Redrive is not turned on for {name}. An admin adds it to the dead-letter queues to redrive in Settings, "
            "under AWS.")


def not_a_name(name: str) -> str:
    return f"{name[:80]} is not an SQS queue name, so it is not redriven."


def other_account(name: str, account: str, settings_account: str) -> str:
    return (f"{name} is in AWS account {account}, and the AWS settings name account {settings_account}, so it is not "
            "redriven.")


def other_account_of_credentials(name: str, account: str, credentials_account: str) -> str:
    return (f"{name} is in AWS account {account}, and the server's credentials are in account {credentials_account}, "
            "so it is not redriven. An admin sets the AWS account ID in Settings, under AWS.")


def other_region(name: str, region: str, allowed: list) -> str:
    return (f"{name} is in {region}, and the AWS settings allow {', '.join(allowed) or 'no region'}, so it is not "
            "redriven. An admin adds the region in Settings, under AWS.")


def no_caller_account(e: BaseException) -> str:
    n = aws_reads.error_name(e)
    if n in ("NoCredentialsError", "PartialCredentialsError", "CredentialRetrievalError"):
        return aws_reader.NO_CREDENTIALS
    return (f"The credentials' AWS account could not be found (sts:GetCallerIdentity failed with {n}), so nothing is "
            "redriven. Set the AWS account ID in Settings, under AWS.")


def place(settings: dict, dlq: dict, credentials_account: Optional[str], home_region: Optional[str]) -> dict:
    """Where SQS is called for an open gate (QV-740): ``{"ok": True, "account", "region"}``, or ``{"ok": False,
    "code": "no_account", "reason"}``. ``credentials_account``: sts:GetCallerIdentity's, needed when the settings
    name no account. ``home_region``: the settings' region, else boto3's default."""
    name = dlq.get("name") or ""
    account = settings.get("accountId") or credentials_account
    if not account:
        return {"ok": False, "code": "no_account", "reason": aws_reader.NO_CREDENTIALS}
    if dlq.get("account") and dlq["account"] != account:
        text = other_account(name, dlq["account"], account) if settings.get("accountId") else \
            other_account_of_credentials(name, dlq["account"], account)
        return {"ok": False, "code": "no_account", "reason": text}
    home = settings.get("region") or home_region
    allowed = [r for r in [home, *(settings.get("regions") or [])] if r]
    region = dlq.get("region") or home
    if not region:
        return {"ok": False, "code": "no_account", "reason": aws_reader.NO_REGION}
    if region not in allowed:
        return {"ok": False, "code": "no_account", "reason": other_region(name, region, allowed)}
    return {"ok": True, "account": account, "region": region}


def gate(settings: dict, dlq: dict, *, installed: bool, can_manage: bool) -> dict:
    """Whether this dead-letter queue may be redriven now, and if not why, in one sentence, without calling AWS.
    ``dlq``: ``{name, account, region}`` (account and region from its messages' spans, None when not known).
    ``{"ok": True, "account", "region"}`` (the settings' account and region; None when they name none), or ``{"ok":
    False, "code", "reason"}``. The codes: not_configured (AWS off, or no boto3), dlq_not_named, no_account,
    not_manager. Someone who can't manage gets not_manager as soon as AWS is on, whatever the queue (QV-788).
    Before SQS is called, ``place`` checks the account and region against the credentials' own."""
    def no(code: str, reason: str) -> dict:
        return {"ok": False, "code": code, "reason": reason}

    name = dlq.get("name") or ""
    if not aws_settings.is_on(settings):
        return no("not_configured", OFF)
    # before anything that tells the server's setup: whether boto3 is there, the redrive list, the account and the
    # regions the settings allow are for an admin (QV-788)
    if not can_manage:
        return no("not_manager", NOT_MANAGER)
    if not installed:
        return no("not_configured", NO_BOTO3)
    if not aws_settings.is_queue_name(name):
        return no("dlq_not_named", not_a_name(name))
    if not any(af.name_matches(p, name) for p in settings.get("redrive") or []):
        return no("dlq_not_named", not_named(name))
    account, mine = dlq.get("account"), settings.get("accountId")
    if account and mine and account != mine:
        return no("no_account", other_account(name, account, mine))
    region, home, others = dlq.get("region"), settings.get("region"), settings.get("regions") or []
    if region and home and region != home and region not in others:
        return no("no_account", other_region(name, region, [home, *others]))
    return {"ok": True, "account": mine, "region": home}


# ── what AWS answered, in words ──────────────────────────────────────────────────────────────────────────────────

def _message(e: BaseException) -> str:
    resp = getattr(e, "response", None)
    if isinstance(resp, dict):
        m = (resp.get("Error") or {}).get("Message")
        if m:
            return str(m)
    return str(e) if str(e) != aws_reads.error_name(e) else ""


def _err_text(e: BaseException) -> str:
    m = _message(e).rstrip(". ")
    return f": {m}" if m and m != aws_reads.error_name(e) else ""


def _denied(e: BaseException) -> bool:
    n = aws_reads.error_name(e)
    return n.startswith("AccessDenied") or n == "InvalidClientTokenId"


def _no_queue(e: BaseException) -> bool:
    return aws_reads.error_name(e) in ("ResourceNotFoundException", "QueueDoesNotExist",
                                       "AWS.SimpleQueueService.NonExistentQueue")


#: every action the move needs, by its lowercase name: AWS's denial names the action in lowercase
_KNOWN = {a.lower(): a for a in REDRIVE_ON_DLQ + REDRIVE_ON_SOURCE + ("kms:Decrypt", "kms:GenerateDataKey")}
_DENIED = re.compile(r"not authorized to perform:?\s*([a-z0-9-]+:[a-z0-9*]+)(?:\s+on resource:?\s*(arn:[^\s,;]+))?",
                     re.I)


def denied_action(e: Any) -> Optional[dict]:
    """The action and resource AWS's AccessDenied message names, when it names them. SQS refuses a move at once when
    the credentials lack an action the move itself needs, and its message names that action, which is often not the
    one called: "... is not authorized to perform: sqs:receivemessage on resource: arn:...-dlq ..."."""
    text = e if isinstance(e, str) else _message(e) if isinstance(e, BaseException) else ""
    m = _DENIED.search(text or "")
    if not m:
        return None
    return {"action": _KNOWN.get(m.group(1).lower(), m.group(1)),
            "resource": re.sub(r"[.)]+$", "", m.group(2)) if m.group(2) else None}


def denied_text(e: Any, called: str, arn: str) -> str:
    """What to say when AWS denied a redrive call: the action AWS named and where to allow it, or the call itself
    when AWS named nothing."""
    q = name_of(arn)
    d = denied_action(e)
    if not d:
        return (f"AWS denied {called} on {q}. Check that the server's AWS credentials allow redrive on {q} and on the "
                "queues its messages came from.")
    on = name_of(d["resource"]) if d["resource"] else q
    where = f" on {d['resource']}" if d["resource"] else ""
    why = "" if d["action"] == called else f", which {called} needs to move the messages"
    return f"AWS denied {d['action']} on {on}{why}. Allow {d['action']}{where} for the server's AWS credentials."


def _failure(e: BaseException, what: str, arn: str) -> str:
    q = name_of(arn)
    call = "sqs:StartMessageMoveTask" if what == "start" else "sqs:ListMessageMoveTasks"
    n = aws_reads.error_name(e)
    if n in ("NoCredentialsError", "PartialCredentialsError", "CredentialRetrievalError"):
        return ("No AWS credentials were found. Give the server credentials the usual way: environment variables, "
                "a profile, or an instance or task role.")
    if _denied(e):
        if what == "start" or denied_action(e):
            return denied_text(e, call, arn)
        return f"AWS denied reading {q}'s moves. Allow sqs:ListMessageMoveTasks on {q} for the server's credentials."
    if _no_queue(e):
        return f"SQS has no queue {q} in this account and region."
    if n == "UnsupportedOperation":
        return (f"SQS refused to move messages from {q}{_err_text(e)}. Only a queue that is some queue's dead-letter "
                "queue can be redriven, and only one move can run on it at a time.")
    return f"{call} failed on {q} with {n}{_err_text(e)}."


# ── move tasks ───────────────────────────────────────────────────────────────────────────────────────────────────

def _num(v: Any) -> Optional[int]:
    if v is None or v == "" or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return int(f) if f == f and f not in (float("inf"), float("-inf")) else None


def _ms(v: Any) -> Optional[int]:
    """StartedTimestamp: epoch ms from SQS (boto3 gives the number as is)."""
    if hasattr(v, "timestamp"):
        return int(v.timestamp() * 1000)
    return _num(v)


def _task(r: dict) -> dict:
    return {"handle": r.get("TaskHandle"), "status": str(r.get("Status") or "UNKNOWN"),
            "moved": _num(r.get("ApproximateNumberOfMessagesMoved")),
            "toMove": _num(r.get("ApproximateNumberOfMessagesToMove")),
            "failure": r.get("FailureReason"), "startedAt": _ms(r.get("StartedTimestamp")),
            "destination": r.get("DestinationArn")}


def move_tasks(sqs: Any, arn: str) -> list:
    """The latest move tasks on a dead-letter queue, newest first (SQS keeps up to 10). Raises what boto3 raises."""
    out = sqs.list_message_move_tasks(SourceArn=arn, MaxResults=10)
    return [_task(r) for r in (out or {}).get("Results") or []]


def move_task(sqs: Any, arn: str, handle: Optional[str], started_at: int) -> Optional[dict]:
    """One move's status, by its handle. SQS gives a handle only while the task runs, so a finished one is found by
    the start time closest to when it was asked for, within a minute."""
    tasks = move_tasks(sqs, arn)
    for t in tasks:
        if handle and t["handle"] == handle:
            return t
    near = [t for t in tasks if t["startedAt"] is not None and abs(t["startedAt"] - started_at) < 60_000]
    return min(near, key=lambda t: abs(t["startedAt"] - started_at)) if near else None


def _attrs(sqs: Any, url: str, names: list) -> dict:
    return (sqs.get_queue_attributes(QueueUrl=url, AttributeNames=names) or {}).get("Attributes") or {}


def preview(sqs: Any, arn: str, url: str) -> dict:
    """What a redrive would move, for the confirmation: ``{"ok": True, "preview": {visible, inFlight, running}}``
    (running: a move already running, or None), or ``{"ok": False, "reason"}``. Reads only."""
    try:
        a = _attrs(sqs, url, ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])
        running = next((t for t in move_tasks(sqs, arn) if t["status"] == "RUNNING"), None)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "reason": _failure(e, "read", arn)}
    return {"ok": True, "preview": {"visible": _num(a.get("ApproximateNumberOfMessages")),
                                    "inFlight": _num(a.get("ApproximateNumberOfMessagesNotVisible")),
                                    "running": running}}


def start(sqs: Any, arn: str, max_per_second: Optional[int] = None) -> dict:
    """Starts moving every message in the dead-letter queue back to the queue it came from: no destination is given,
    so SQS sends each message to its own source queue. A move already running is not doubled. ``{"ok": True,
    "handle"}`` or ``{"ok": False, "refused", "reason"}`` (refused: nothing was asked of SQS)."""
    try:
        running = next((t for t in move_tasks(sqs, arn) if t["status"] == "RUNNING"), None)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "refused": False, "reason": _failure(e, "read", arn)}
    if running:
        return {"ok": False, "refused": True, "reason": f"A redrive of {name_of(arn)} is already running: "
                                                        f"{counted(running['moved'], running['toMove']) or '0 moved'}."}
    try:
        kw = {"MaxNumberOfMessagesPerSecond": max_per_second} if max_per_second else {}
        out = sqs.start_message_move_task(SourceArn=arn, **kw)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "refused": False, "reason": _failure(e, "start", arn)}
    return {"ok": True, "handle": (out or {}).get("TaskHandle")}


def cancel(sqs: Any, arn: str, handle: str) -> dict:
    """Asks SQS to cancel a running move. Messages already moved stay in their source queue. It is a request, not a
    stop: SQS keeps moving for a while, and the count it answers with is not the final one, so the log reads the count
    later. ``{"ok": True}`` or ``{"ok": False, "reason"}``."""
    try:
        sqs.cancel_message_move_task(TaskHandle=handle)
    except Exception as e:  # noqa: BLE001
        if _denied(e):
            return {"ok": False, "reason": denied_text(e, "sqs:CancelMessageMoveTask", arn)}
        if _no_queue(e):
            return {"ok": False, "reason": "SQS has no running move with that handle. It may have finished."}
        return {"ok": False, "reason": f"sqs:CancelMessageMoveTask failed with {aws_reads.error_name(e)}"
                                       f"{_err_text(e)}."}
    return {"ok": True}


def queue_depth(sqs: Any, url: str) -> Optional[int]:
    """Messages in a queue now, visible, in flight and delayed; None when SQS gave no count. Raises what boto3
    raises."""
    a = _attrs(sqs, url, ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible",
                          "ApproximateNumberOfMessagesDelayed"])
    n = [_num(a.get(k)) for k in ("ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible",
                                  "ApproximateNumberOfMessagesDelayed")]
    return None if all(x is None for x in n) else sum(x or 0 for x in n)
