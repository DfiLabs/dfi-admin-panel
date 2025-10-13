import json, os, urllib.parse
import boto3
from datetime import datetime, timezone, timedelta

SES_FROM = os.environ.get("SES_FROM", "hello@dfi-labs.com")
SES_REGION = os.environ.get("SES_REGION")
BASE_URL = os.environ.get("BASE_URL", "https://pulse.dfi-labs.com/pulse/access/verify.html")
TABLE = os.environ.get("TABLE", "pulse_tokens")

ses = boto3.client("ses", region_name=SES_REGION) if SES_REGION else boto3.client("ses")
ddb = boto3.client("dynamodb")

HTML_TMPL = (
    "<p>Hi {name},</p>"
    "<p>Your access link to DFI Labs Pulse:</p>"
    "<p><a href=\"{link}\">Open dashboard</a></p>"
    "<p>This link expires in 72 hours.</p>"
)
TEXT_TMPL = (
    "Hi {name},\n\n"
    "Your access link to DFI Labs Pulse:\n{link}\n\n"
    "This link expires in 72 hours.\n"
)

def _resp(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
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
        exp = now + timedelta(hours=72)
        ddb.put_item(TableName=TABLE, Item={
            "token": {"S": token},
            "email": {"S": email},
            "name": {"S": name},
            "entity": {"S": entity},
            "issuedAt": {"S": now.isoformat()},
            "expiresAt": {"S": exp.isoformat()},
        })

        link = f"{BASE_URL}?t={urllib.parse.quote(token)}&next={urllib.parse.quote(nextp)}&email={urllib.parse.quote(email)}&name={urllib.parse.quote(name)}&entity={urllib.parse.quote(entity)}"

        try:
            ses.send_email(
                Source=SES_FROM,
                Destination={"ToAddresses": [email]},
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


