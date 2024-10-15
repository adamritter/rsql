
import json
from fasthtml.common import *
from fasttag import *

# check that htmx exists
if not os.path.exists("static/htmx.min.js"):
    raise Exception("static/htmx.min.js not found")

# [charset, viewport, htmxscr,surrsrc,scopesrc]
htmxscr   = Script(src="/static/htmx.min.js")
htmxwsscr = Script(src="/static/ws.js")
surrsrc   = Script(src="/static/surreal.js")
scopesrc  = Script(src="/static/css-scope-inline.js")
picocss   = Link(rel="stylesheet", href="/static/pico.min.css")
viewport  = Meta(name="viewport", content="width=device-width, initial-scale=1, viewport-fit=cover")
charset   = Meta(charset="utf-8")
static_hdrs = [charset, viewport, htmxscr, htmxwsscr, surrsrc, scopesrc]

def clr_input(): return Script("document.querySelector('input').value = ''")

def collapsible(content, open=False):
    return Div(B("v" if open else ">",
                 onclick="this.textContent = this.textContent === '>' ? 'v' : '>'; this.nextElementSibling.style.display = this.nextElementSibling.style.display === 'none' ? 'block' : 'none'"),
                Span(content, style="display:block" if open else "display:none"),
                )

def show_json(data):
    if isinstance(data, dict):
            return collapsible(Table(*[Tr(Td(k), Td(show_json(v)), valign="top") for k,v in data.items()]), open=(len(data) < 10))
    if isinstance(data, list): return collapsible(Ul(*[Li(show_json(d)) for d in data]), open=(len(data) < 5))
    if isinstance(data, (int, float)): return Span(str(data), style="color:darkgreen")
    if isinstance(data, str): return Span(data)
    return data

def show_table(t):
    return Table(
        Tr(*[Th(c.name) for c in t.columns]),
        *[Tr(*[Td(show_json(json.loads(row[c.name])) if "json" in c.name and row[c.name] else str(row[c.name])) for c in t.columns]) for row in t.rows]
    )

def show_db(db):
    dic = {}
    for t in db.tables:
        dic[t.name] = show_table(t)
    return show_json(dic)
    return ""


def loadsvalues(d): return {k: json.loads(v) if v and ("json" in k) else v for k,v in d.items()}
def table_rows(db, name): return [loadsvalues(row) for row in db.table(name).rows]
def Trd(*d, **kw): return Tr(*[Td(v) for v in d], **kw)
def apply_if(cond, f, x): return f(x) if cond(x) else x
