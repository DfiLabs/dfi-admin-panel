import json, os, urllib.parse
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone, timedelta

SES_FROM = os.environ.get("SES_FROM", "hello@dfi-labs.com")
SES_REGION = os.environ.get("SES_REGION")
SES_BCC = os.environ.get("BCC", "")
BASE_URL = os.environ.get("BASE_URL", "https://pulse.dfi-labs.com/pulse/access/verify.html")
TABLE = os.environ.get("TABLE", "pulse_tokens")
USERS_TABLE = os.environ.get("USERS_TABLE", "pulse_users")

ses = boto3.client("ses", region_name=SES_REGION) if SES_REGION else boto3.client("ses")
ddb = boto3.client("dynamodb")

LOGO_URL = os.environ.get("LOGO_URL", "https://pulse.dfi-labs.com/pulse/access/logo.png")
PRIMARY = "#8b5cf6"
HTML_TMPL = (
    "<div style=\"font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;max-width:560px;margin:auto;color:#0f172a\">"
    f"<div style=\"padding:16px 0; text-align:center\"><img src=\"{LOGO_URL}\" alt=\"DFI Labs\" style=\"height:36px\"/></div>"
    "<div style=\"background:linear-gradient(180deg,#f8fafc,#f1f5f9);border:1px solid #e2e8f0;border-radius:12px;padding:20px\">"
    "<p style=\"margin:0 0 12px\">Hi {name},</p>"
    "<p style=\"margin:0 0 16px;color:#334155\">Your access link to DFI Labs Pulse to check out Descartes live in action:</p>"
    f"<p style=\"margin:0 0 16px\"><a href=\"{{link}}\" style=\"display:inline-block;padding:12px 18px;background:{PRIMARY};color:white;text-decoration:none;border-radius:999px;font-weight:600\">Open dashboard</a></p>"
    "<p style=\"margin:0;color:#475569\">This link expires in 48 hours.</p>"
    "</div>"
    "<div style=\"text-align:center;color:#94a3b8;font-size:12px;margin-top:12px\">DFI Labs</div>"
    "</div>"
)
TEXT_TMPL = (
    "Hi {name},\n\n"
    "Your access link to DFI Labs Pulse to check out Descartes live in action:\n{link}\n\n"
    "This link expires in 48 hours.\n"
)

def _resp(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            # Do NOT set Access-Control-Allow-Origin here to avoid duplicate header with
            # Lambda Function URL CORS. Function URL config injects the Origin header.
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "POST,OPTIONS"
        },
        "body": json.dumps(body)
    }

def lambda_handler(event, context):
    # CORS preflight
    if event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS":
        return _resp(200, {})
    try:
        body = json.loads(event.get("body") or "{}")
        email = (body.get("email") or "").strip()
        name = (body.get("name") or "there").strip()
        entity = (body.get("entity") or "").strip()
        nextp = (body.get("next") or "/signal-dashboard/").strip()
        if not email or not entity:
            return _resp(400, {"error": "missing fields"})

        token = os.urandom(16).hex()
        now = datetime.now(timezone.utc)
        exp = now + timedelta(hours=48)
        # TTL value in epoch seconds for DynamoDB TTL
        ttl = int(exp.timestamp())

        # Throttle duplicate sends: combine atomic guard item + recent scan.
        # 1) Atomic guard keyed per email with TTL 120s prevents concurrent double sends.
        guard_key = f"send#{email.lower()}"
        try:
            ddb.put_item(
                TableName=TABLE,
                Item={
                    "token": {"S": guard_key},
                    "email": {"S": email},
                    "issuedAt": {"S": now.isoformat()},
                    "ttl": {"N": str(int((now + timedelta(seconds=120)).timestamp()))},
                    "status": {"S": "guard"}
                },
                ConditionExpression="attribute_not_exists(#k)",
                ExpressionAttributeNames={"#k": "token"}
            )
        except ClientError as ce:
            if ce.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return _resp(200, {"ok": True, "throttled": True})
        # 2) Extra safety: recent scan (helps just after TTL expiry)
        try:
            cutoff_iso = (now - timedelta(seconds=120)).isoformat()
            scan = ddb.scan(
                TableName=TABLE,
                FilterExpression="email = :e AND issuedAt >= :cut",
                ExpressionAttributeValues={
                    ":e": {"S": email},
                    ":cut": {"S": cutoff_iso}
                }
            )
            recent = [it for it in (scan.get("Items") or []) if it.get("status",{}).get("S") == "issued"]
            if recent:
                return _resp(200, {"ok": True, "throttled": True})
        except Exception:
            pass
        ddb.put_item(TableName=TABLE, Item={
            "token": {"S": token},
            "email": {"S": email},
            "name": {"S": name},
            "entity": {"S": entity},
            "issuedAt": {"S": now.isoformat()},
            "expiresAt": {"S": exp.isoformat()},
            "ttl": {"N": str(ttl)},
            "status": {"S": "issued"},
            "next": {"S": nextp},
        })

        # Upsert persistent users aggregator with counts
        try:
            ddb.update_item(
                TableName=USERS_TABLE,
                Key={"email": {"S": email.lower()}},
                UpdateExpression=(
                    "ADD issuedCount :one SET #n=if_not_exists(#n,:n), entity=if_not_exists(entity,:y), "
                    "firstIssued = if_not_exists(firstIssued,:t), lastIssued=:t"
                ),
                ExpressionAttributeNames={"#n": "name"},
                ExpressionAttributeValues={
                    ":one": {"N": "1"},
                    ":n": {"S": name or ""},
                    ":y": {"S": entity or ""},
                    ":t": {"S": now.isoformat()}
                }
            )
        except Exception:
            pass

        link = f"{BASE_URL}?t={urllib.parse.quote(token)}&next={urllib.parse.quote(nextp)}&email={urllib.parse.quote(email)}&name={urllib.parse.quote(name)}&entity={urllib.parse.quote(entity)}"

        try:
            dest = {"ToAddresses": [email]}
            if SES_BCC:
                dest["BccAddresses"] = [SES_BCC]
            ses.send_email(
                Source=SES_FROM,
                Destination=dest,
                Message={
                    "Subject": {"Data": "DFI Labs Pulse — Your access link"},
                    "Body": {
                        "Text": {"Data": TEXT_TMPL.format(name=name, link=link)},
                        "Html": {"Data": HTML_TMPL.format(name=name, link=link)}
                    }
                }
            )
            return _resp(200, {"ok": True})
        except Exception as e:
            # In SES sandbox, return link so the UI can show a copyable URL
            return _resp(200, {"ok": False, "link": link, "ses_error": str(e)})
    except Exception as e:
        return _resp(500, {"error": str(e)})


