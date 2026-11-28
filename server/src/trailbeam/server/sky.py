"""The sign-in pages' frame (QV-666): the hosted app's auth pages, which sit in the landing page's pre-dawn sky.

Sign in, the token page, Create your admin account, Choose your password, Sign-in failed and Not allowed are
rendered here by the server, not by the shared UI's shell: they answer before anyone is signed in, and they work
with or without the shell's build and without scripts. So each page is one response: its styles are inline, the
logo and icons are inline SVG, and nothing is fetched (the shell's files under /ui/ are behind sign-in). The colors
are @trailbeam/ui's sky tokens (ui/src/design/tokens.css). The families are the design system's, Newsreader for the
wordmark and headings, Instrument Sans for labels and prose and IBM Plex Mono for values only, used when the
browser has them; otherwise the system's serif, sans and mono stand in.
"""
from __future__ import annotations

from html import escape

from . import __version__, logo

Response = tuple[int, dict, bytes]

# The sky tokens, as tokens.css names them (test_sky.py checks the values against it).
TOKENS = {
    "sky": "#0a0c22", "sky-2": "#12153a", "sky-line": "#2b2f63", "sky-ink": "#f6f1e7", "sky-ink-2": "#c6c5e0",
    "sky-ink-3": "#9a99c2", "wait": "#93a5ff", "ember": "#ffb65e", "lost": "#ff7667", "dawn": "#ff9d7a",
    "peak": "#121035",
}

CSS = "html{" + "".join(f"--{k}:{v};" for k, v in TOKENS.items()) + r"""
--serif:"Newsreader",ui-serif,Georgia,serif;--sans:"Instrument Sans",ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif;
--mono:"IBM Plex Mono",ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;color-scheme:dark}
*{box-sizing:border-box}
body{margin:0;min-height:100vh;background:var(--sky);color:var(--sky-ink);font:400 1rem/1.6 var(--sans);
  -webkit-font-smoothing:antialiased}
.sky{position:fixed;inset:0;z-index:0;pointer-events:none;
  background:
    radial-gradient(2px 2px at 12% 18%,#fff 50%,transparent 51%),radial-gradient(2px 2px at 27% 9%,#fff 50%,transparent 51%),
    radial-gradient(2.5px 2.5px at 41% 23%,#fff 50%,transparent 51%),radial-gradient(2px 2px at 58% 12%,#fff 50%,transparent 51%),
    radial-gradient(2px 2px at 66% 31%,#fff 50%,transparent 51%),radial-gradient(2.5px 2.5px at 79% 7%,#fff 50%,transparent 51%),
    radial-gradient(2px 2px at 88% 26%,#fff 50%,transparent 51%),radial-gradient(2px 2px at 94% 14%,#fff 50%,transparent 51%),
    radial-gradient(2px 2px at 71% 44%,rgba(255,255,255,.7) 50%,transparent 51%),
    radial-gradient(2px 2px at 84% 51%,rgba(255,255,255,.6) 50%,transparent 51%),
    radial-gradient(120% 60% at 75% 100%,rgba(255,157,122,.30),rgba(255,182,94,.10) 35%,transparent 65%),
    linear-gradient(to bottom,var(--sky) 0%,var(--sky) 35%,var(--sky-2) 70%,#2a2350 100%)}
.sky:after{content:"";position:absolute;left:0;right:0;bottom:0;height:22vh;background:var(--peak);
  clip-path:polygon(0 70%,9% 52%,18% 63%,30% 38%,41% 58%,52% 46%,61% 60%,70% 30%,74% 28%,78% 33%,88% 55%,100% 44%,100% 100%,0 100%)}
@media (min-width:64rem){.sky:before{content:"";position:absolute;inset:0;
  background:linear-gradient(to right,rgba(10,12,34,.92) 0%,rgba(10,12,34,.7) 38%,rgba(10,12,34,.15) 75%,transparent)}}
.top,main,footer{position:relative;z-index:1;margin:0 auto;max-width:75rem;padding-left:1rem;padding-right:1rem}
@media (min-width:48rem){.top,main,footer{padding-left:2.5rem;padding-right:2.5rem}}
.top{padding-top:1.5rem}.top a{display:inline-block;color:var(--sky-ink)}
.top svg{display:block;height:1.625rem;width:auto}
main{padding-top:2rem;padding-bottom:5rem}@media (min-width:48rem){main{padding-top:3rem}}
.auth{max-width:26rem;padding-top:1rem}@media (min-width:64rem){.auth{padding-top:2.5rem}}
h1{margin:0;font:400 2.75rem/1 var(--serif);letter-spacing:-.02em}@media (min-width:48rem){h1{font-size:3.5rem}}
p{margin:0}.lead{margin-top:1.25rem;color:var(--sky-ink-2)}
a{color:var(--dawn);text-underline-offset:4px;text-decoration:none}a:hover{text-decoration:underline}
code,.mono{font-family:var(--mono);font-size:.8125rem}
.ways{margin-top:2rem;display:grid;gap:.625rem}
.btn,.quiet{display:inline-flex;align-items:center;justify-content:center;gap:.625rem;height:2.75rem;padding:0 1.25rem;
  border-radius:999px;font:500 .875rem/1 var(--sans);cursor:pointer;text-decoration:none;transition:background-color .15s,border-color .15s}
.btn{border:0;background:var(--sky-ink);color:var(--sky);font-weight:600}.btn:hover{background:#fff;text-decoration:none}
.quiet{width:100%;border:1px solid rgba(246,241,231,.35);background:none;color:var(--sky-ink)}
.quiet:hover{border-color:var(--sky-ink);text-decoration:none}
.btn:disabled,.quiet:disabled{cursor:not-allowed;opacity:.6}
.btn:focus-visible,.quiet:focus-visible,a:focus-visible,input:focus-visible{outline:2px solid var(--wait);outline-offset:2px}
.divider{display:flex;align-items:center;gap:.75rem;margin:1.5rem 0;color:var(--sky-ink-3);font-size:.75rem;line-height:1.4}
.divider:before,.divider:after{content:"";height:1px;flex:1;background:var(--sky-line)}
form.auth-form{display:grid;gap:1rem}form.auth-form.spaced{margin-top:2rem}
.field{display:grid;gap:.375rem}.field label{font-size:.875rem;font-weight:500;color:var(--sky-ink)}
.field input{height:2.75rem;border-radius:.75rem;border:1px solid var(--sky-line);background:rgba(10,12,34,.7);
  padding:0 .875rem;font:400 1rem var(--sans);color:var(--sky-ink);outline:none;transition:border-color .15s,background-color .15s}
.field input.mono{font-family:var(--mono);font-size:.875rem}
.field input::placeholder{color:var(--sky-ink-3)}.field input:hover{border-color:var(--sky-ink-3)}
.field input:focus{border-color:var(--sky-ink-2);background:var(--sky)}
.hint{font-size:.75rem;line-height:1.4;color:var(--sky-ink-3)}
.actions{margin-top:.25rem;display:flex;flex-wrap:wrap;align-items:center;gap:.75rem 1.25rem}
.notice{margin-top:1.5rem;border-radius:.75rem;background:rgba(255,118,103,.15);padding:.625rem .875rem;font-size:.875rem;
  line-height:1.45;color:var(--sky-ink)}
form .notice,.ways .notice{margin-top:0}
.links{margin-top:2rem;display:flex;flex-wrap:wrap;gap:.75rem 1.5rem;font-size:.875rem;font-weight:500}
footer{padding-bottom:1.5rem;display:flex;flex-wrap:wrap;gap:.25rem 1rem;font-size:.75rem;line-height:1.4;color:var(--sky-ink-3)}
footer a{color:var(--sky-ink-3)}footer a:hover{color:var(--sky-ink)}
[hidden]{display:none!important}
"""

LOGO = (f'<svg viewBox="14 48 {logo.LOCKUP_W} {logo.H}" role="img" aria-label="Trailbeam">'
        f'<path fill="currentColor" d="{logo.DOME}"/><path fill="var(--ember)" d="{logo.DOT}"/>'
        f'<path fill="currentColor" d="{logo.NAME}"/></svg>')

# The hosted provider buttons' icons (trailbeam-app components/auth/ui.tsx): GitHub's mark and lucide's KeyRound.
GITHUB_MARK = ('<svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true" fill="currentColor"><path d="M8 0C3.58 0 0 '
               "3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94"
               "-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28"
               "-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64"
               "-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15"
               " 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016"
               ' 8c0-4.42-3.58-8-8-8z"/></svg>')
KEY_ICON = ('<svg width="16" height="16" viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" '
            'stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M2.586 17.414A2 2 0 0 0 2 18.828V21a1 1 '
            "0 0 0 1 1h3a1 1 0 0 0 1-1v-1a1 1 0 0 1 1-1h1a1 1 0 0 0 1-1v-1a1 1 0 0 1 1-1h.172a2 2 0 0 0 1.414-.586l.814-.814"
            'a6.5 6.5 0 1 0-4-4z"/><circle cx="16.5" cy="7.5" r=".5" fill="currentColor"/></svg>')


def page(status: int, title: str, body: str) -> Response:
    """A whole page in the sky: the logo (a link to /), ``body`` in the left column, and the version and health."""
    html = ('<!doctype html><html lang="en"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex">'
            f"<title>{escape(title)} - Trailbeam</title><style>{CSS}</style></head><body>"
            '<div class="sky" aria-hidden="true"></div>'
            f'<header class="top"><a href="/" aria-label="Trailbeam">{LOGO}</a></header>'
            f'<main><div class="auth">{body}</div></main>'
            f'<footer><span>Trailbeam <span class="mono">{escape(__version__)}</span></span>'
            '<a href="/api/health">Server health</a></footer></body></html>')
    return status, {"Content-Type": "text/html; charset=utf-8"}, html.encode()


def field(label: str, input_html: str, *, hint: str = "", row_id: str = "") -> str:
    """A label above its input, and a hint under it. ``input_html`` is the <input> with id ``f-…`` (the label's for)."""
    fid = input_html.split('id="', 1)[1].split('"', 1)[0]
    row = f' id="{row_id}"' if row_id else ""
    return (f'<div class="field"{row}>'
            f'<label for="{fid}">{escape(label)}</label>{input_html}'
            + (f'<p class="hint">{escape(hint)}</p>' if hint else "") + "</div>")


def notice(text: str, el_id: str = "", hidden: bool = False) -> str:
    """What went wrong, in a tinted box that screen readers announce."""
    attrs = (f' id="{el_id}"' if el_id else "") + ' role="alert"' + (" hidden" if hidden else "")
    return f'<p class="notice"{attrs}>{escape(text)}</p>'
