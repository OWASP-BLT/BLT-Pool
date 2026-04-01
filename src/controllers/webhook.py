import json
from js import console, Response
from core.crypto import verify_signature
from core.github_client import get_installation_token
from controllers.issue_handlers import handle_issue_comment, handle_issue_opened, handle_issue_labeled
from controllers.pr_handlers import handle_pull_request_opened, handle_pull_request_closed, handle_pull_request_review_submitted, handle_workflow_run, handle_check_run, check_unresolved_conversations
from controllers.peer_review import handle_pull_request_review, handle_pull_request_for_review
from models.leaderboard import _track_pr_reopened_in_d1
from views.pages import _json


async def handle_webhook(request, env) -> Response:
    """Verify the GitHub webhook signature and route to the correct handler."""
    body_text = await request.text()
    payload_bytes = body_text.encode("utf-8")

    # Extract header metadata immediately so every webhook invocation is logged.
    delivery_id = request.headers.get("X-GitHub-Delivery", "")
    event = request.headers.get("X-GitHub-Event", "")

    signature = request.headers.get("X-Hub-Signature-256") or ""
    secret = (getattr(env, "WEBHOOK_SECRET", "") or "").strip()
    if not secret:
        console.error(
            "[BLT][webhook] "
            f"delivery={delivery_id or '-'} event={event or '-'} method={request.method} "
            "status=rejected_missing_webhook_secret"
        )
        return _json(
            {
                "error": "Webhook authentication is not configured (missing WEBHOOK_SECRET)",
                "code": "webhook_secret_missing",
            },
            503,
        )

    if not verify_signature(payload_bytes, signature, secret):
        console.log(
            "[BLT][webhook] "
            f"delivery={delivery_id or '-'} event={event or '-'} method={request.method} status=rejected_invalid_signature"
        )
        return _json({"error": "Invalid signature"}, 401)

    # Parse payload only after signature verification
    try:
        payload = json.loads(body_text)
    except Exception:
        console.log(
            "[BLT][webhook] "
            f"delivery={delivery_id or '-'} event={event or '-'} method={request.method} status=rejected_invalid_json"
        )
        return _json({"error": "Invalid JSON"}, 400)

    action = payload.get("action", "") if isinstance(payload, dict) else ""
    installation_id = ((payload.get("installation") or {}).get("id") if isinstance(payload, dict) else None)
    repo_full_name = ((payload.get("repository") or {}).get("full_name") if isinstance(payload, dict) else "")
    sender_login = ((payload.get("sender") or {}).get("login") if isinstance(payload, dict) else "")
    issue_number = ((payload.get("issue") or {}).get("number") if isinstance(payload, dict) else None)
    pr_number = ((payload.get("pull_request") or {}).get("number") if isinstance(payload, dict) else None)
    item_number = issue_number or pr_number or ""

    console.log(
        "[BLT][webhook] "
        f"delivery={delivery_id or '-'} event={event or '-'} action={action or '-'} "
        f"repo={repo_full_name or '-'} sender={sender_login or '-'} item={item_number or '-'} "
        f"installation={installation_id or '-'} method={request.method} status=received"
    )

    app_id = getattr(env, "APP_ID", "")
    private_key = getattr(env, "PRIVATE_KEY", "")

    if event == "ping":
        return _json({"ok": True, "message": "pong"})

    needs_token = False
    if event == "issue_comment" and action == "created":
        needs_token = True
    elif event == "issues" and action in ("opened", "labeled"):
        needs_token = True
    elif event == "pull_request" and action in ("opened", "synchronize", "reopened", "closed"):
        needs_token = True
    elif event == "pull_request_review" and action in ("submitted", "dismissed"):
        needs_token = True
    elif event in ("pull_request_review_comment", "pull_request_review_thread"):
        needs_token = True
    elif event == "workflow_run":
        needs_token = True
    elif event == "check_run" and action in ("created", "completed"):
        needs_token = True

    if not needs_token:
        return _json({"ok": True, "ignored": True})

    token = None
    if installation_id and app_id and private_key:
        token = await get_installation_token(installation_id, app_id, private_key)

    if not token:
        console.error("[BLT] Could not obtain installation token")
        return _json({"error": "Authentication failed"}, 500)

    blt_api_url = getattr(env, "BLT_API_URL", "https://blt-api.owasp-blt.workers.dev")

    try:
        if event == "issue_comment" and action == "created":
            await handle_issue_comment(payload, token, env)
        elif event == "issues":
            if action == "opened":
                await handle_issue_opened(payload, token, blt_api_url)
            elif action == "labeled":
                await handle_issue_labeled(payload, token, blt_api_url, env=env)
        elif event == "pull_request":
            if action == "opened":
                await handle_pull_request_opened(payload, token, env)
                await handle_pull_request_for_review(payload, token)
            elif action == "synchronize":
                await handle_pull_request_for_review(payload, token)
            elif action == "reopened":
                await _track_pr_reopened_in_d1(payload, env)
                await handle_pull_request_for_review(payload, token)
            elif action == "closed":
                await handle_pull_request_closed(payload, token, env)
        elif event == "pull_request_review":
            if action == "submitted":
                # Preserve existing D1 review-credit tracking
                await handle_pull_request_review_submitted(payload, env)
                # Also check peer review status
                await handle_pull_request_review(payload, token)
            elif action == "dismissed":
                await handle_pull_request_review(payload, token)
        elif event == "pull_request_review_comment":
            await check_unresolved_conversations(payload, token)
        elif event == "pull_request_review_thread":
            await check_unresolved_conversations(payload, token)
        elif event == "workflow_run":
            await handle_workflow_run(payload, token)
        elif event == "check_run" and action in ("created", "completed"):
            await handle_check_run(payload, token)

    except Exception as exc:
        console.error(f"[BLT] Webhook handler error: {exc}")
        return _json({"error": "Internal server error"}, 500)

    return _json({"ok": True})
