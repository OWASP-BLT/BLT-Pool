"""HTML rendering and response helpers for BLT-Pool."""

import html as _html_mod
import json
import time
from typing import Callable, Optional
from urllib.parse import quote

try:
    from js import Headers, Response
except ImportError:
    # Fallback for testing outside Cloudflare Workers
    Headers = Response = None

from index_template import GITHUB_PAGE_HTML


_CALLBACK_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>BLT-Pool GitHub App — Installed!</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link
    rel="stylesheet"
    href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css"
    crossorigin="anonymous"
    referrerpolicy="no-referrer"
  />
</head>
<body class="min-h-screen flex items-center justify-center" style="background:#111827;color:#e5e7eb;">
  <div class="text-center rounded-xl p-12 max-w-md w-full mx-4" style="background:#1F2937;border:1px solid #374151;">
    <div class="w-16 h-16 rounded-full flex items-center justify-center mx-auto mb-6" style="background:rgba(225,1,1,0.1);">
      <i class="fa-solid fa-circle-check text-3xl" style="color:#E10101;" aria-hidden="true"></i>
    </div>
    <h1 class="text-2xl font-bold text-white mb-4">Installation complete!</h1>
    <p class="leading-relaxed mb-6" style="color:#9ca3af;">
      The BLT-Pool GitHub App has been successfully installed on your organization.<br />
      GitHub automation is now active inside BLT-Pool.
    </p>
    <a
      href="https://owaspblt.org"
      target="_blank"
      rel="noopener"
      style="color:#E10101;"
      onmouseover="this.style.textDecoration='underline'" onmouseout="this.style.textDecoration='none'"
    >
      Visit OWASP BLT <i class="fa-solid fa-arrow-right text-xs" aria-hidden="true"></i>
    </a>
  </div>
</body>
</html>
"""


class RenderingService:
    """Render HTML pages and HTTP responses for the worker."""

    def __init__(self, time_ago_fn: Optional[Callable[[int], str]] = None):
        self._time_ago_fn = time_ago_fn or self.time_ago

    def time_ago(self, ts: int) -> str:
        """Return a human-readable 'X time ago' string for a Unix timestamp."""
        diff = int(time.time()) - ts
        if diff < 60:
            return "just now"
        if diff < 3600:
            minutes = diff // 60
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        if diff < 86400:
            hours = diff // 3600
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        if diff < 86400 * 30:
            days = diff // 86400
            return f"{days} day{'s' if days != 1 else ''} ago"
        if diff < 86400 * 365:
            months = diff // (86400 * 30)
            return f"{months} month{'s' if months != 1 else ''} ago"
        years = diff // (86400 * 365)
        return f"{years} year{'s' if years != 1 else ''} ago"

    def json_response(self, data, status: int = 200) -> Response:
        return Response.new(
            json.dumps(data),
            status=status,
            headers=Headers.new({
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            }.items()),
        )

    def html_response(self, html: str, status: int = 200) -> Response:
        return Response.new(
            html,
            status=status,
            headers=Headers.new({"Content-Type": "text/html; charset=utf-8"}.items()),
        )

    def secret_vars_status_html(self, env) -> str:
        """Generate HTML rows showing whether each secret/config variable is set."""
        set_badge = (
            '<span class="inline-flex items-center gap-1.5 font-semibold" style="color:#4ade80">'
            '<i class="fa-solid fa-circle-check" aria-hidden="true"></i> Set'
            "</span>"
        )
        missing_badge = (
            '<span class="inline-flex items-center gap-1.5 font-semibold" style="color:#f87171">'
            '<i class="fa-solid fa-circle-xmark" aria-hidden="true"></i> Not set'
            "</span>"
        )
        optional_badge = (
            '<span class="inline-flex items-center gap-1.5 font-semibold" style="color:#9ca3af">'
            '<i class="fa-solid fa-circle-minus" aria-hidden="true"></i> Not configured'
            "</span>"
        )

        required_vars = ["APP_ID", "PRIVATE_KEY", "WEBHOOK_SECRET"]
        optional_vars = ["GITHUB_CLIENT_ID", "GITHUB_CLIENT_SECRET"]

        rows = [
            '        <div class="mt-4 border-t border-[#E5E5E5] pt-3">',
            '          <p class="mb-1 text-xs font-semibold uppercase tracking-wider text-gray-500">Secret Variables</p>',
            "        </div>",
        ]
        for name in required_vars:
            badge = set_badge if bool(getattr(env, name, "")) else missing_badge
            rows.append(
                f'        <div class="flex items-center justify-between border-b border-[#E5E5E5] py-3 text-sm">'
                f'<span class="text-gray-700"><code class="text-xs">{name}</code></span>'
                f"{badge}</div>"
            )
        for name in optional_vars:
            badge = set_badge if bool(getattr(env, name, "")) else optional_badge
            rows.append(
                f'        <div class="flex items-center justify-between border-b border-[#E5E5E5] py-3 text-sm">'
                f'<span class="text-gray-700"><code class="text-xs">{name}</code>'
                f' <span class="text-[0.7rem] text-gray-500">(optional)</span></span>'
                f"{badge}</div>"
            )
        return "\n".join(rows)

    def github_app_html(self, app_slug: str, env=None) -> str:
        install_url = (
            f"https://github.com/apps/{app_slug}/installations/new"
            if app_slug
            else "https://github.com/apps/blt-github-app/installations/new"
        )
        year = time.gmtime().tm_year
        secret_vars_html = self.secret_vars_status_html(env) if env is not None else ""
        return (
            GITHUB_PAGE_HTML
            .replace("{{INSTALL_URL}}", install_url)
            .replace("{{YEAR}}", str(year))
            .replace("{{SECRET_VARS_STATUS}}", secret_vars_html)
        )

    def landing_html(self, app_slug: str, env=None) -> str:
        return self.github_app_html(app_slug, env)

    def callback_html(self) -> str:
        return _CALLBACK_HTML

    def generate_mentor_row(self, mentor: dict, stats: Optional[dict] = None) -> str:
        """Generate HTML for a single mentor list row."""
        name = _html_mod.escape(mentor.get("name", "Unknown"))
        github = mentor.get("github_username", "")
        specialties = mentor.get("specialties", [])
        max_mentees = mentor.get("max_mentees", 3)
        timezone = mentor.get("timezone", "")
        status = mentor.get("status", "available")
        active = mentor.get("active", True)

        avatar_url = (
            f"https://github.com/{github}.png"
            if github
            else "https://api.dicebear.com/7.x/initials/svg?seed=" + quote(name)
        )

        if not active or status == "inactive":
            status_badge = '<span class="inline-flex items-center gap-1 rounded-full border border-gray-200 bg-gray-50 px-2 py-0.5 text-xs font-semibold text-gray-500">Inactive</span>'
        elif status == "assigned":
            status_badge = '<span class="inline-flex items-center gap-1 rounded-full border border-blue-200 bg-blue-50 px-2 py-0.5 text-xs font-semibold text-blue-700">Mentoring</span>'
        else:
            status_badge = '<span class="inline-flex items-center gap-1 rounded-full border border-emerald-200 bg-emerald-50 px-2 py-0.5 text-xs font-semibold text-emerald-700">Available</span>'

        specialty_chips = " ".join(
            f'<span class="rounded bg-gray-100 px-1.5 py-0.5 text-xs text-gray-600">{specialty}</span>'
            for specialty in specialties
        ) if specialties else '<span class="text-xs text-gray-400">—</span>'

        github_link = (
            f'<a href="https://github.com/{github}" target="_blank" rel="noopener" '
            f'class="text-gray-500 hover:text-[#E10101]" aria-label="{name} GitHub profile">'
            '<i class="fa-brands fa-github" aria-hidden="true"></i></a>'
            if github
            else '<span class="text-gray-300"><i class="fa-brands fa-github" aria-hidden="true"></i></span>'
        )

        tz_cell = (
            f'<span class="text-xs text-gray-500">{_html_mod.escape(timezone)}</span>'
            if timezone
            else '<span class="text-xs text-gray-400">—</span>'
        )

        if stats:
            merged_prs = int(stats.get("merged_prs") or 0)
            reviews = int(stats.get("reviews") or 0)
            stats_desktop = (
                f'<div class="text-center">'
                f'  <p class="text-xs text-gray-400 leading-none">PRs</p>'
                f'  <p class="text-sm font-semibold text-gray-700">{merged_prs}</p>'
                f'</div>'
                f'<div class="text-center">'
                f'  <p class="text-xs text-gray-400 leading-none">Reviews</p>'
                f'  <p class="text-sm font-semibold text-gray-700">{reviews}</p>'
                f'</div>'
            )
            stats_mobile = (
                f'<span class="text-xs text-gray-500">'
                f'<i class="fa-solid fa-code-pull-request text-gray-400" aria-hidden="true"></i> {merged_prs} PRs</span>'
                f'<span class="text-xs text-gray-500">'
                f'<i class="fa-solid fa-magnifying-glass-chart text-gray-400" aria-hidden="true"></i> {reviews} reviews</span>'
            )
            desktop_cols = "sm:grid-cols-[1fr_auto_auto_auto_auto_auto_auto]"
        else:
            stats_desktop = ""
            stats_mobile = ""
            desktop_cols = "sm:grid-cols-[1fr_auto_auto_auto_auto]"

        return f'''
    <li class="flex items-start gap-3 rounded-xl border border-[#E5E5E5] bg-white px-4 py-3 transition hover:shadow-sm sm:items-center sm:gap-4">
      <img src="{avatar_url}" alt="{name}" class="mt-0.5 h-9 w-9 shrink-0 rounded-full border border-[#E5E5E5] bg-white object-cover sm:mt-0 sm:h-10 sm:w-10">
      <div class="min-w-0 flex-1">
        <div class="hidden sm:grid {desktop_cols} sm:items-center sm:gap-4">
          <div class="min-w-0">
            <p class="truncate font-semibold text-[#111827] text-sm">{name}</p>
            <div class="mt-0.5 flex flex-wrap gap-1">{specialty_chips}</div>
          </div>
          <div>{status_badge}</div>
          <div class="text-center">
            <p class="text-xs text-gray-400 leading-none">Cap</p>
            <p class="text-sm font-semibold text-gray-700">{max_mentees}</p>
          </div>
          {stats_desktop}
          <div>{tz_cell}</div>
          <div>{github_link}</div>
        </div>
        <div class="sm:hidden">
          <div class="flex items-start justify-between gap-2">
            <p class="truncate font-semibold text-[#111827] text-sm">{name}</p>
            <div class="shrink-0">{github_link}</div>
          </div>
          <div class="mt-0.5 flex flex-wrap gap-1">{specialty_chips}</div>
          <div class="mt-1.5 flex flex-wrap items-center gap-x-2 gap-y-1">
            {status_badge}
            <span class="text-xs text-gray-500">Cap: {max_mentees}</span>
            {stats_mobile}
            {tz_cell}
          </div>
        </div>
      </div>
    </li>
    '''

    def build_referral_leaderboard(self, mentors: list) -> list:
        """Return a sorted list of (referrer_username, count) tuples."""
        counts: dict = {}
        for mentor in mentors:
            ref = mentor.get("referred_by", "").strip()
            if ref:
                counts[ref] = counts.get(ref, 0) + 1
        return sorted(counts.items(), key=lambda item: item[1], reverse=True)

    def index_html(
        self,
        mentors: list = None,
        mentor_stats: Optional[dict] = None,
        active_assignments: Optional[list] = None,
        assignment_comment_stats: Optional[dict] = None,
    ) -> str:
        """Generate the BLT-Pool mentor directory homepage."""
        if mentors is None:
            mentors = []
        if mentor_stats is None:
            mentor_stats = {}
        if active_assignments is None:
            active_assignments = []
        if assignment_comment_stats is None:
            assignment_comment_stats = {}

        mentor_stats_lower = {key.lower(): value for key, value in mentor_stats.items()}
        year = time.gmtime().tm_year
        mentor_count = len(mentors)
        available_count = len([
            mentor for mentor in mentors
            if mentor.get("active", True) and mentor.get("status", "available") == "available"
        ])

        mentor_rows_html = "\n".join(
            self.generate_mentor_row(
                mentor,
                mentor_stats_lower.get(mentor.get("github_username", "").lower()),
            )
            for mentor in mentors
        )

        if active_assignments:
            def assignment_item(assignment: dict) -> str:
                mentor_login = _html_mod.escape(assignment["mentor_login"])
                mentee_raw = assignment.get("mentee_login", "")
                mentee_login = _html_mod.escape(mentee_raw)
                org = _html_mod.escape(assignment["org"])
                repo = _html_mod.escape(assignment["issue_repo"])
                number = _html_mod.escape(str(assignment["issue_number"]))
                assigned_time_ago = _html_mod.escape(self._time_ago_fn(assignment["assigned_at"]))
                mentor_comments = assignment_comment_stats.get(assignment["mentor_login"], 0)
                mentee_comments = assignment_comment_stats.get(mentee_raw, 0) if mentee_raw else 0

                mentee_html = ""
                if mentee_login:
                    mentee_html = f'''
              <div class="flex items-center gap-2 min-w-0">
                <img src="https://github.com/{mentee_login}.png"
                     alt="{mentee_login}"
                     class="h-7 w-7 shrink-0 rounded-full border border-[#E5E5E5]">
                <div class="min-w-0">
                  <p class="text-xs text-gray-400 leading-none">Mentee</p>
                  <a href="https://github.com/{mentee_login}" target="_blank" rel="noopener"
                     class="text-sm font-semibold text-[#111827] hover:text-[#E10101] truncate block">
                    @{mentee_login}
                  </a>
                </div>
                <span class="shrink-0 rounded-full bg-gray-100 px-2 py-0.5 text-xs font-semibold text-gray-600" title="Total comments">{mentee_comments} pts</span>
              </div>'''

                return f'''<li class="rounded-xl border border-[#E5E5E5] bg-gray-50 p-4">
              <div class="flex flex-wrap items-center justify-between gap-3">
                <div class="flex items-center gap-2 min-w-0">
                  <img src="https://github.com/{mentor_login}.png"
                       alt="{mentor_login}"
                       class="h-7 w-7 shrink-0 rounded-full border border-[#E5E5E5]">
                  <div class="min-w-0">
                    <p class="text-xs text-gray-400 leading-none">Mentor</p>
                    <a href="https://github.com/{mentor_login}" target="_blank" rel="noopener"
                       class="text-sm font-semibold text-[#111827] hover:text-[#E10101] truncate block">
                      @{mentor_login}
                    </a>
                  </div>
                  <span class="shrink-0 rounded-full bg-[#feeae9] px-2 py-0.5 text-xs font-semibold text-[#E10101]" title="Total comments">{mentor_comments} pts</span>
                </div>
                {mentee_html}
                <a href="https://github.com/{org}/{repo}/issues/{number}"
                   target="_blank" rel="noopener"
                   class="inline-flex items-center gap-1.5 rounded-full bg-[#feeae9] px-3 py-1 text-xs font-semibold text-[#E10101] hover:bg-red-100 transition shrink-0">
                  <i class="fa-brands fa-github text-xs" aria-hidden="true"></i>
                  {org}/{repo}#{number}
                </a>
              </div>
              <p class="mt-2 text-xs text-gray-400">
                <i class="fa-regular fa-clock mr-1" aria-hidden="true"></i>Assigned {assigned_time_ago}
              </p>
            </li>'''

            assignment_items = "\n".join(assignment_item(assignment) for assignment in active_assignments)
            active_assignments_html = f'''
    <section id="active-assignments" class="rounded-2xl border border-[#E5E5E5] bg-white p-7 sm:p-9">
      <div class="mb-5 flex items-center gap-3">
        <div class="inline-flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-[#feeae9] text-[#E10101]">
          <i class="fa-solid fa-link" aria-hidden="true"></i>
        </div>
        <div>
          <h3 class="text-2xl font-bold text-[#111827]">Active Mentor Assignments</h3>
          <p class="mt-0.5 text-sm text-gray-500">{len(active_assignments)} active assignment{"s" if len(active_assignments) != 1 else ""}</p>
        </div>
      </div>
      <ul class="space-y-3">
        {assignment_items}
      </ul>
    </section>'''
        else:
            active_assignments_html = ""

        leaderboard_rows = self.build_referral_leaderboard(mentors)
        if leaderboard_rows:
            lb_items = "\n".join(
                f'''<li class="flex items-center justify-between gap-2 py-1.5 border-b border-[#E5E5E5] last:border-0">
              <a href="https://github.com/{ref}" target="_blank" rel="noopener"
                 class="flex items-center gap-2 text-sm font-medium text-gray-700 hover:text-[#E10101] truncate">
                <img src="https://github.com/{ref}.png" alt="{ref}" class="h-6 w-6 rounded-full border border-[#E5E5E5]">
                @{ref}
              </a>
              <span class="shrink-0 rounded-full bg-[#feeae9] px-2 py-0.5 text-xs font-bold text-[#E10101]">{count}</span>
            </li>'''
                for ref, count in leaderboard_rows[:10]
            )
            leaderboard_html = f'''
        <section class="rounded-2xl border border-[#E5E5E5] bg-white p-6 h-fit sticky top-24">
          <div class="mb-4 flex items-center gap-2">
            <div class="inline-flex h-8 w-8 items-center justify-center rounded-lg bg-[#feeae9] text-[#E10101]">
              <i class="fa-solid fa-trophy text-sm" aria-hidden="true"></i>
            </div>
            <h3 class="text-lg font-bold text-[#111827]">Referral Leaderboard</h3>
          </div>
          <p class="mb-4 text-xs text-gray-500">GitHub users who have referred the most mentors to the pool.</p>
          <ol class="space-y-0">
            {lb_items}
          </ol>
        </section>'''
        else:
            leaderboard_html = '''
        <section class="rounded-2xl border border-[#E5E5E5] bg-white p-6 h-fit sticky top-24">
          <div class="mb-4 flex items-center gap-2">
            <div class="inline-flex h-8 w-8 items-center justify-center rounded-lg bg-[#feeae9] text-[#E10101]">
              <i class="fa-solid fa-trophy text-sm" aria-hidden="true"></i>
            </div>
            <h3 class="text-lg font-bold text-[#111827]">Referral Leaderboard</h3>
          </div>
          <p class="text-sm text-gray-500">No referrals yet — be the first to invite a mentor!</p>
        </section>'''

        return f'''<!DOCTYPE html>
<html lang="en" class="scroll-smooth">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="description" content="BLT-Pool for OWASP BLT. Connect with mentors and install the BLT GitHub extension.">
  <title>BLT-Pool | OWASP BLT Mentor Directory</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css" crossorigin="anonymous" referrerpolicy="no-referrer">
  <script>
    tailwind.config = {{
      theme: {{
        extend: {{
          colors: {{
            'blt-primary': '#E10101',
            'blt-primary-hover': '#b91c1c',
            'blt-border': '#E5E5E5'
          }},
          fontFamily: {{
            sans: ['Plus Jakarta Sans', 'ui-sans-serif', 'system-ui', 'sans-serif']
          }}
        }}
      }}
    }}
  </script>
  <style>
    body {{
      background:
        radial-gradient(circle at 0% 0%, rgba(225, 1, 1, 0.09), transparent 32%),
        radial-gradient(circle at 95% 4%, rgba(225, 1, 1, 0.05), transparent 28%),
        #f8fafc;
    }}
  </style>
</head>
<body class="min-h-screen font-sans text-gray-900 antialiased">

  <header class="sticky top-0 z-40 border-b border-[#E5E5E5] bg-white/90 backdrop-blur">
    <div class="mx-auto flex w-full max-w-7xl flex-wrap items-center justify-between gap-3 px-4 py-3 sm:px-6 lg:px-8">
      <a href="/" class="flex items-center gap-3" aria-label="BLT-Pool home">
        <img src="/logo-sm.png" alt="OWASP BLT logo" class="h-10 w-10 rounded-xl border border-[#E5E5E5] bg-white object-contain p-1">
        <div>
          <p class="text-sm font-semibold uppercase tracking-wide text-gray-500">OWASP BLT</p>
          <h1 class="text-lg font-extrabold text-[#111827]">BLT-Pool</h1>
        </div>
      </a>
      <nav class="order-3 flex w-full items-center justify-center gap-0.5 rounded-xl border border-[#E5E5E5] bg-white p-1 sm:order-none sm:w-auto sm:justify-start" aria-label="Primary">
        <a href="/" class="rounded-lg bg-[#feeae9] px-2 py-1.5 text-xs font-semibold text-[#E10101] sm:px-3 sm:py-2 sm:text-sm">Mentors</a>
        <a href="/github-app" class="rounded-lg px-2 py-1.5 text-xs font-semibold text-gray-700 hover:bg-gray-50 sm:px-3 sm:py-2 sm:text-sm">GitHub App</a>
        <a href="https://owaspblt.org" target="_blank" rel="noopener" class="rounded-lg px-2 py-1.5 text-xs font-semibold text-gray-700 hover:bg-gray-50 sm:px-3 sm:py-2 sm:text-sm">
          OWASP BLT <i class="fa-solid fa-arrow-up-right-from-square text-xs" aria-hidden="true"></i>
        </a>
      </nav>
      <div class="order-2 flex items-center gap-2 sm:order-none">
        <span role="status" aria-label="Service status: Operational"
              class="inline-flex items-center gap-1 rounded-full border border-emerald-200 bg-emerald-50 px-2.5 py-1 text-[11px] font-semibold text-emerald-700">
          <i class="fa-solid fa-circle text-[0.4rem]" aria-hidden="true"></i>
          Live
        </span>
        <a href="/admin/login"
           class="inline-flex items-center gap-1.5 rounded-md border border-[#E5E5E5] px-3 py-2 text-xs font-semibold text-gray-700 transition hover:border-[#E10101] hover:bg-[#feeae9] hover:text-[#E10101] focus:outline-none focus:ring-2 focus:ring-red-600 focus:ring-offset-2">
          <i class="fa-solid fa-shield-halved text-[#E10101]" aria-hidden="true"></i>
          Admin
        </a>
      </div>
    </div>
  </header>

  <main class="mx-auto flex-1 w-full max-w-7xl space-y-10 px-4 py-10 sm:px-6 lg:px-8">

    <section class="overflow-hidden rounded-3xl border border-[#E5E5E5] bg-white p-7 shadow-[0_14px_40px_rgba(225,1,1,0.10)] sm:p-10">
      <div class="grid gap-8 lg:grid-cols-2 lg:items-center">
        <div>
          <span class="mb-4 inline-flex items-center gap-2 rounded-full border border-[#E5E5E5] bg-gray-50 px-3 py-1 text-xs font-semibold text-gray-700">
            <i class="fa-solid fa-users text-[#E10101]" aria-hidden="true"></i>
            Mentor directory for BLT contributors
          </span>
          <h2 class="text-3xl font-extrabold leading-tight text-[#111827] sm:text-5xl">
            Find your guide inside
            <span class="text-[#E10101]">OWASP BLT-Pool</span>
          </h2>
          <p class="mt-4 max-w-2xl text-base leading-relaxed text-gray-600 sm:text-lg">
            Connect with mentors, get support for your first pull request, and keep contribution quality high with a practical community workflow.
          </p>
          <div class="mt-7 flex flex-wrap gap-3">
            <a href="https://owasp.slack.com/signup" target="_blank" rel="noopener"
               class="inline-flex items-center gap-2 rounded-md bg-[#E10101] px-5 py-3 text-sm font-semibold text-white transition hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-red-600 focus:ring-offset-2">
              Join OWASP Slack
            </a>
            <a href="#mentor-commands"
               class="inline-flex items-center gap-2 rounded-md border border-[#E5E5E5] px-5 py-3 text-sm font-semibold text-gray-700 transition hover:border-[#E10101] hover:bg-[#feeae9] hover:text-[#E10101]">
              View Commands
            </a>
          </div>
        </div>
        <div class="grid gap-4 sm:grid-cols-2">
          <article class="rounded-2xl border border-[#E5E5E5] bg-gray-50 p-5">
            <p class="text-xs font-semibold uppercase tracking-[0.22em] text-gray-400">Mentors</p>
            <p class="mt-3 text-4xl font-extrabold text-[#111827]">{mentor_count}</p>
            <p class="mt-2 text-sm text-gray-600">Contributors available to help across docs, frontend, backend, security, and more.</p>
          </article>
          <article class="rounded-2xl border border-[#E5E5E5] bg-gray-50 p-5">
            <p class="text-xs font-semibold uppercase tracking-[0.22em] text-gray-400">Available Now</p>
            <p class="mt-3 text-4xl font-extrabold text-[#E10101]">{available_count}</p>
            <p class="mt-2 text-sm text-gray-600">Ready for fresh assignments across the BLT ecosystem.</p>
          </article>
          <article class="rounded-2xl border border-[#E5E5E5] bg-gray-50 p-5 sm:col-span-2">
            <p class="text-xs font-semibold uppercase tracking-[0.22em] text-gray-400">Matching Model</p>
            <p class="mt-3 text-lg font-bold text-[#111827]">Capacity-aware and mentor-friendly</p>
            <p class="mt-2 text-sm text-gray-600">Round-robin assignment prevents overload and keeps responses timely.</p>
          </article>
        </div>
      </div>
    </section>

    <div class="grid grid-cols-1 gap-8 lg:grid-cols-[2fr_1fr] lg:items-start">
      <section class="space-y-4">
        <div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <h3 class="text-2xl font-bold text-[#111827]">
            Mentor Pool <span class="text-base font-medium text-gray-500">({mentor_count} total, {available_count} available)</span>
          </h3>
        </div>
        <ul class="space-y-2" aria-label="Mentor list">
          <li class="hidden sm:grid sm:grid-cols-[1fr_auto_auto_auto_auto] sm:items-center sm:gap-4 sm:px-4 sm:py-1 text-xs font-semibold uppercase tracking-wide text-gray-400">
            <span>Mentor</span>
            <span>Status</span>
            <span class="text-center">Cap</span>
            <span>Timezone</span>
            <span>Link</span>
          </li>
          {mentor_rows_html}
        </ul>
      </section>

      {leaderboard_html}
    </div>

    {active_assignments_html}

    <section class="rounded-2xl border border-[#E5E5E5] bg-white p-7 sm:p-9">
      <h3 class="text-2xl font-bold text-[#111827]">How Mentor Matching Works</h3>
      <div class="mt-6 grid gap-5 md:grid-cols-3">
        <article class="rounded-xl border border-[#E5E5E5] bg-gray-50 p-5">
          <div class="mb-3 inline-flex h-10 w-10 items-center justify-center rounded-lg bg-[#feeae9] text-[#E10101]">
            <i class="fa-solid fa-user-plus" aria-hidden="true"></i>
          </div>
          <h4 class="text-base font-bold text-[#111827]">1. Pick an issue</h4>
          <p class="mt-2 text-sm text-gray-600">Start with an issue tagged for contribution or mentor support.</p>
        </article>
        <article class="rounded-xl border border-[#E5E5E5] bg-gray-50 p-5">
          <div class="mb-3 inline-flex h-10 w-10 items-center justify-center rounded-lg bg-[#feeae9] text-[#E10101]">
            <i class="fa-solid fa-arrows-rotate" aria-hidden="true"></i>
          </div>
          <h4 class="text-base font-bold text-[#111827]">2. Get assigned</h4>
          <p class="mt-2 text-sm text-gray-600">Mentors are matched with healthy load balancing to avoid bottlenecks.</p>
        </article>
        <article class="rounded-xl border border-[#E5E5E5] bg-gray-50 p-5">
          <div class="mb-3 inline-flex h-10 w-10 items-center justify-center rounded-lg bg-[#feeae9] text-[#E10101]">
            <i class="fa-solid fa-comments" aria-hidden="true"></i>
          </div>
          <h4 class="text-base font-bold text-[#111827]">3. Build and review</h4>
          <p class="mt-2 text-sm text-gray-600">Work with your mentor on review quality, security checks, and merge confidence.</p>
        </article>
      </div>
    </section>

    <section id="mentor-commands" class="rounded-2xl border border-[#E5E5E5] bg-white p-7 sm:p-9">
      <h3 class="text-2xl font-bold text-[#111827]">Mentor Slash Commands</h3>
      <p class="mt-3 text-sm leading-relaxed text-gray-600">
        Use these commands directly in GitHub issue comments to interact with the mentor system.
      </p>
      <div class="mt-5 grid gap-4 sm:grid-cols-2">
        <article class="rounded-xl border border-[#E5E5E5] bg-gray-50 p-4">
          <p class="font-mono text-sm font-bold text-[#E10101]">/mentor</p>
          <p class="mt-2 text-sm text-gray-600">Request a mentor for this issue. The bot auto-assigns the best available mentor from the pool.</p>
        </article>
        <article class="rounded-xl border border-[#E5E5E5] bg-gray-50 p-4">
          <p class="font-mono text-sm font-bold text-[#E10101]">/unmentor</p>
          <p class="mt-2 text-sm text-gray-600">Cancel a mentor assignment. Use this to undo an accidental <code class="font-mono">/mentor</code> request. Available to the issue author or the assigned mentor.</p>
        </article>
        <article class="rounded-xl border border-[#E5E5E5] bg-gray-50 p-4">
          <p class="font-mono text-sm font-bold text-[#E10101]">/mentor-pause</p>
          <p class="mt-2 text-sm text-gray-600">Pause your mentor availability. Use this when you need a break from accepting new mentees.</p>
        </article>
        <article class="rounded-xl border border-[#E5E5E5] bg-gray-50 p-4">
          <p class="font-mono text-sm font-bold text-[#E10101]">/handoff</p>
          <p class="mt-2 text-sm text-gray-600">Transfer this issue to another available mentor. The current mentor uses this to hand off cleanly.</p>
        </article>
        <article class="rounded-xl border border-[#E5E5E5] bg-gray-50 p-4">
          <p class="font-mono text-sm font-bold text-[#E10101]">/rematch</p>
          <p class="mt-2 text-sm text-gray-600">Request a different mentor for this issue. The bot re-runs matching to find a fresh assignment.</p>
        </article>
      </div>
    </section>

    <section id="join-mentor" class="rounded-2xl border border-[#E5E5E5] bg-white p-7 sm:p-9">
      <div class="mb-6 flex items-start gap-4">
        <div class="inline-flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-[#feeae9] text-[#E10101]">
          <i class="fa-solid fa-user-plus" aria-hidden="true"></i>
        </div>
        <div>
          <h3 class="text-2xl font-bold text-[#111827]">Become a Mentor</h3>
          <p class="mt-1 text-sm leading-relaxed text-gray-600">
            Fill in the form and submit — you are added to the mentor pool immediately.
          </p>
        </div>
      </div>
      <form id="mentor-form" class="grid gap-4 sm:grid-cols-2" novalidate>
        <div>
          <label for="mf-name" class="mb-1 block text-sm font-semibold text-gray-700">
            Display Name <span class="text-[#E10101]">*</span>
          </label>
          <input id="mf-name" type="text" required autocomplete="name" placeholder="Jane Doe"
                 maxlength="100"
                 class="w-full rounded-md border border-gray-300 px-3 py-2 text-sm text-gray-900 placeholder:text-gray-400 focus:border-[#E10101] focus:ring-1 focus:ring-[#E10101] focus:outline-none">
        </div>
        <div>
          <label for="mf-github" class="mb-1 block text-sm font-semibold text-gray-700">
            GitHub Username <span class="text-[#E10101]">*</span>
          </label>
          <input id="mf-github" type="text" required autocomplete="username" placeholder="janedoe"
                 maxlength="39" pattern="[a-zA-Z0-9]([a-zA-Z0-9\\-]{{0,37}}[a-zA-Z0-9])?"
                 class="w-full rounded-md border border-gray-300 px-3 py-2 text-sm text-gray-900 placeholder:text-gray-400 focus:border-[#E10101] focus:ring-1 focus:ring-[#E10101] focus:outline-none">
        </div>
        <div class="sm:col-span-2">
          <label for="mf-specialties" class="mb-1 block text-sm font-semibold text-gray-700">
            Specialties <span class="text-xs font-normal text-gray-400">(optional — comma-separated)</span>
          </label>
          <input id="mf-specialties" type="text" placeholder="e.g. frontend, python, security, docs"
                 maxlength="300"
                 class="w-full rounded-md border border-gray-300 px-3 py-2 text-sm text-gray-900 placeholder:text-gray-400 focus:border-[#E10101] focus:ring-1 focus:ring-[#E10101] focus:outline-none">
        </div>
        <div>
          <label for="mf-max" class="mb-1 block text-sm font-semibold text-gray-700">
            Max concurrent mentees
          </label>
          <input id="mf-max" type="number" min="1" max="10" value="3"
                 class="w-full rounded-md border border-gray-300 px-3 py-2 text-sm text-gray-900 focus:border-[#E10101] focus:ring-1 focus:ring-[#E10101] focus:outline-none">
        </div>
        <div>
          <label for="mf-tz" class="mb-1 block text-sm font-semibold text-gray-700">
            Timezone <span class="text-xs font-normal text-gray-400">(optional)</span>
          </label>
          <input id="mf-tz" type="text" placeholder="e.g. UTC+5:30"
                 maxlength="60"
                 class="w-full rounded-md border border-gray-300 px-3 py-2 text-sm text-gray-900 placeholder:text-gray-400 focus:border-[#E10101] focus:ring-1 focus:ring-[#E10101] focus:outline-none">
        </div>
        <div class="sm:col-span-2">
          <label for="mf-referral" class="mb-1 block text-sm font-semibold text-gray-700">
            Referred By <span class="text-xs font-normal text-gray-400">(optional — GitHub username of who invited you)</span>
          </label>
          <input id="mf-referral" type="text" placeholder="e.g. janedoe"
                 maxlength="39" pattern="[a-zA-Z0-9]([a-zA-Z0-9\\-]{{0,37}}[a-zA-Z0-9])?"
                 class="w-full rounded-md border border-gray-300 px-3 py-2 text-sm text-gray-900 placeholder:text-gray-400 focus:border-[#E10101] focus:ring-1 focus:ring-[#E10101] focus:outline-none">
        </div>
        <div id="mf-error" role="alert" class="hidden sm:col-span-2 text-sm font-semibold text-[#E10101]"></div>
        <div id="mf-success" role="status" class="hidden sm:col-span-2 text-sm font-semibold text-green-600"></div>
        <div class="sm:col-span-2">
          <button id="mf-submit" type="submit"
                  class="inline-flex items-center gap-2 rounded-md bg-[#E10101] px-5 py-3 text-sm font-semibold text-white transition hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-red-600 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed">
            <i class="fa-solid fa-user-plus" aria-hidden="true"></i>
            Join the Mentor Pool
          </button>
        </div>
      </form>
      <script>
        (function () {{
          var GH_USERNAME_RE = /^[a-zA-Z0-9](?:[a-zA-Z0-9\\-]{{0,37}}[a-zA-Z0-9])?$/;
          var SPECIALTY_RE = /^[a-z0-9][a-z0-9+#.\\-]{{0,29}}$/;

          function containsScripting(val) {{
            if (/[<>"&]/.test(val)) return true;
            if (/javascript\\s*:/i.test(val)) return true;
            if (/on\\w+\\s*=/i.test(val)) return true;
            return false;
          }}

          document.getElementById('mentor-form').addEventListener('submit', function (e) {{
            e.preventDefault();
            var name     = document.getElementById('mf-name').value.trim();
            var github   = document.getElementById('mf-github').value.trim().replace(/^@/, '');
            var specs    = document.getElementById('mf-specialties').value.trim();
            var maxM     = parseInt(document.getElementById('mf-max').value.trim(), 10);
            var tz       = document.getElementById('mf-tz').value.trim();
            var referral = document.getElementById('mf-referral').value.trim().replace(/^@/, '');
            var errEl    = document.getElementById('mf-error');
            var okEl     = document.getElementById('mf-success');
            var btn      = document.getElementById('mf-submit');
            errEl.classList.add('hidden');
            okEl.classList.add('hidden');

            if (!name) {{
              errEl.textContent = 'Display name is required.';
              errEl.classList.remove('hidden');
              return;
            }}
            if (!github) {{
              errEl.textContent = 'GitHub username is required.';
              errEl.classList.remove('hidden');
              return;
            }}
            if (name.length > 100) {{
              errEl.textContent = 'Display name must be 100 characters or fewer.';
              errEl.classList.remove('hidden');
              return;
            }}
            if (tz.length > 60) {{
              errEl.textContent = 'Timezone must be 60 characters or fewer.';
              errEl.classList.remove('hidden');
              return;
            }}
            if (containsScripting(name)) {{
              errEl.textContent = 'Display name contains invalid characters. HTML and scripting are not allowed.';
              errEl.classList.remove('hidden');
              return;
            }}
            if (containsScripting(tz)) {{
              errEl.textContent = 'Timezone contains invalid characters. HTML and scripting are not allowed.';
              errEl.classList.remove('hidden');
              return;
            }}
            if (containsScripting(specs)) {{
              errEl.textContent = 'Specialties contain invalid characters. HTML and scripting are not allowed.';
              errEl.classList.remove('hidden');
              return;
            }}
            if (!GH_USERNAME_RE.test(github)) {{
              errEl.textContent = 'GitHub username may only contain letters, digits, and single hyphens, and cannot begin or end with a hyphen (max 39 characters).';
              errEl.classList.remove('hidden');
              return;
            }}
            if (referral && !GH_USERNAME_RE.test(referral)) {{
              errEl.textContent = 'Referred-by username may only contain letters, digits, and single hyphens, and cannot begin or end with a hyphen (max 39 characters).';
              errEl.classList.remove('hidden');
              return;
            }}

            var specialties = specs ? specs.split(',').map(function(s) {{ return s.trim(); }}).filter(Boolean) : [];
            for (var i = 0; i < specialties.length; i++) {{
              if (!SPECIALTY_RE.test(specialties[i])) {{
                errEl.textContent = 'Invalid specialty tag "' + specialties[i] + '". Tags must be 1-30 lowercase alphanumeric characters (also +, #, ., -).';
                errEl.classList.remove('hidden');
                return;
              }}
            }}

            if (isNaN(maxM) || maxM < 1 || maxM > 10) {{
              errEl.textContent = 'Max concurrent mentees must be a number between 1 and 10.';
              errEl.classList.remove('hidden');
              return;
            }}

            btn.disabled = true;
            fetch('/api/mentors', {{
              method: 'POST',
              headers: {{'Content-Type': 'application/json'}},
              body: JSON.stringify({{
                name: name,
                github_username: github,
                specialties: specialties,
                max_mentees: maxM,
                timezone: tz,
                referred_by: referral
              }})
            }})
            .then(function(res) {{
              return res.json().then(function(data) {{ return {{ok: res.ok, data: data}}; }});
            }})
            .then(function(result) {{
              btn.disabled = false;
              if (result.ok) {{
                if (result.data && result.data.active) {{
                  okEl.textContent = 'Welcome to the mentor pool, @' + github + '! Your mentor profile is now published.';
                }} else {{
                  okEl.textContent = 'Thanks, @' + github + '! Your mentor profile was saved and is waiting for admin publishing.';
                }}
                okEl.classList.remove('hidden');
                document.getElementById('mentor-form').reset();
              }} else {{
                errEl.textContent = result.data.error || 'An error occurred. Please try again.';
                errEl.classList.remove('hidden');
              }}
            }})
            .catch(function() {{
              btn.disabled = false;
              errEl.textContent = 'Network error. Please try again.';
              errEl.classList.remove('hidden');
            }});
          }});
        }}());
      </script>
    </section>

  </main>

  <footer class="border-t border-[#E5E5E5] bg-white">
    <div class="mx-auto max-w-7xl px-4 py-6 text-center text-sm text-gray-600 sm:px-6 lg:px-8">
      Built by the <a href="https://owaspblt.org" target="_blank" rel="noopener" class="text-red-600 hover:underline">OWASP BLT community</a>
      <span aria-hidden="true"> • </span>
      <a href="/github-app" class="text-red-600 hover:underline">GitHub App</a>
      <span aria-hidden="true"> • </span>
      <a href="https://github.com/OWASP-BLT/BLT-Pool" target="_blank" rel="noopener" class="text-red-600 hover:underline">BLT-Pool Repo</a>
      <p class="mt-2 text-xs text-gray-500">&copy; {year} OWASP Foundation. All rights reserved.</p>
    </div>
  </footer>

</body>
</html>'''
