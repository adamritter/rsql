import time
from .helpers import *
from fasttag import *
import fasttag
from rsql import URLM
import threading
from queue import Queue

# Replace global variables with thread-local storage
local = threading.local()
local.tab_id = 0
last_tab_id = 0

# Replace the global queues dict with a thread-safe defaultdict
from collections import defaultdict
queues = defaultdict(Queue)

def append_queue(e):
    queues[local.tab_id].put(e)

def get_and_clear_queue():
    q = list(queues[local.tab_id].queue)
    queues[local.tab_id] = Queue()
    return q

def append_queue_to(tid, e):
    print("append_queue_to", tid, e)
    queues[tid].put(e)

def rt_with_sqlx(rt, app):
    def rtx(route):
        def decorator(func):
            return rt(route)(with_sqlx(func, app))
        return decorator
    return rtx

from inspect import isfunction,ismethod,Parameter,get_annotations
# htmx.config.headers['X-My-Custom-Header'] = 'customValue';

global_app = None

from functools import wraps
from inspect import signature
def with_sqlx(f, app=None):
    @wraps(f)
    def wrapper(*args):
        global last_tab_id
        start_time = time.time()
        sqlx_tab_id = args[-1]
        hx_request = args[-2]
        args = args[:-2]
        
        if hx_request:
            local.tab_id = sqlx_tab_id
        else:
            local.tab_id = last_tab_id
        
        # print("args", args, "hx_request", hx_request, "sqlx_tab_id", sqlx_tab_id, "tab_id", local.tab_id, "app", app)
        global global_app
        global_app = app
        result = f(*args)

        q = list(queues[local.tab_id].queue)
        queues[local.tab_id] = Queue()
        
        end_time = time.time()
        render_time = (end_time - start_time) * 1000  # Convert to milliseconds
        # print("reading queue", local.tab_id, f"Rendering time: {render_time:.2f} ms", "hx_request", hx_request, "read queue", q)
        
        if not hx_request:
            q.append(Script(f"document.body.addEventListener('htmx:configRequest', function(evt) {{ evt.detail.headers['SQLX-Tab-Id'] = '{last_tab_id}';}});"))
            q.append(Meta(name="htmx-config", content='{"defaultSwapStyle":"none"}'))
            last_tab_id += 1
        
        if result:
            return (*q, result)
        else:
            return (*q,)
    
    # Extend the wrapper's signature with the new parameter
    original_sig = signature(f)
    new_params = list(original_sig.parameters.values()) + [Parameter('hx_request', Parameter.KEYWORD_ONLY, default=None, annotation=bool),
        Parameter('sqlx_tab_id', Parameter.KEYWORD_ONLY, default=None, annotation=int)]
    wrapper.__signature__ = original_sig.replace(parameters=new_params)
    
    return wrapper

lastid = 0
def nextid():
    global lastid
    lastid += 1
    return f"e{lastid}"



def table(t, cb, header=None, id=None):
    if not id:
        id = nextid()
    print("table", t)
    r = fasttag.Table(
        Thead(header) if header else None,
         Tbody(*[Tr(cb(row), id=f"e{abs(row.__hash__())}") for row in t], id=id))
    tid = local.tab_id
    print("table2", tid)
    t.on_insert(lambda row: append_queue_to(tid, Template(Tbody(Tr(cb(row), id=f"e{abs(row.__hash__())}"), hx_swap_oob=f"beforeend:#{id}"))))
    t.on_delete(lambda row: append_queue_to(tid, Template(Tr(id=f"e{abs(row.__hash__())}", hx_swap_oob="delete"))))
    # TODO: fix bug in HTML: changing id inside template doesn't work
    t.on_update(lambda old, new: append_queue_to(tid, Template(Tr(cb(new),id=f"e{abs(new.__hash__())}", hx_swap_oob=f"outerHTML: #e{abs(old.__hash__())}"))))
    return r

def ulli(t, cb, header=None, id=None):
    if not id:
        id = nextid()
    r = fasttag.Ul(
        Li(header) if header else None,
        *[Li(cb(row), id=f"e{abs(row.__hash__())}") for row in t],
        id=id
    )
    tid = local.tab_id
    t.on_insert(lambda row: append_queue_to(tid, Li(cb(row), id=f"e{abs(row.__hash__())}", hx_swap_oob=f"beforeend: #{id}")))
    t.on_delete(lambda row: append_queue_to(tid, Li(id=f"e{abs(row.__hash__())}", hx_swap_oob="delete")))
    t.on_update(lambda old, new: append_queue_to(tid, Li(cb(new), id=f"e{abs(new.__hash__())}", hx_swap_oob=f"outerHTML: #e{abs(old.__hash__())}")) or 
            append_queue_to(tid, Script(f"console.log('update {old} {new}');"))
    )
    return r

def value(v):
    id = nextid()
    tid = local.tab_id
    v.onchange(lambda new: append_queue_to(tid, Span(new, id=id, hx_swap_oob=f"true")))
    return Span(v.value, id=id)

def show_if(cond, *args):
    id = nextid()
    tid = local.tab_id
    cond.update_cbs.append(lambda old, new: append_queue_to(tid, Span(args, id=id, hx_swap_oob="true", style=None if new else "display: none;")))
    return Span(args, id=id, style=None if cond.value else "display: none;")


def register_table(rtx, model):
    name = model.name
    @rtx('/'+name+'/{id}')
    def get(req):
        return model.fetchone(id=req.path_params["id"])
    
    @rtx('/'+name+'/{id}')
    def delete(req):
        model.delete(id=req.path_params["id"])
    
    @rtx('/'+name)
    def delete(req):
        model.delete(**req.query_params)

    @rtx('/'+name)
    def patch(req):
        model.update({}, **req.query_params)

    @rtx('/'+name+'/{id}')
    def patch(req):
        model.update({"id": req.path_params["id"]}, **req.query_params)

    @rtx('/'+name)
    def post(req):
        model.insert(**req.query_params)

def register_tables(rtx, db):
    for t in db.tables.values():
        register_table(rtx, t)
import random, string

def random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def post_method_creator(app):
    def decorator(func):
        ws = with_sqlx(func, app)
        name = func.__name__
        if name == "<lambda>":
            name = "lambda"
        url = f"/app/{name}/{random_string(10)}"
        app.post(url)(ws)
        return URLM(url, method="POST")
    return decorator


def Button(text, onclick=None, **kwargs):
    if callable(onclick):
        onclick = post_method_creator(global_app)(onclick)
    if isinstance(onclick, URLM):
        kwargs[f"hx_{onclick.__method__}"] = str(onclick)
        return fasttag.Button(text, **kwargs)
    elif not onclick:
        return fasttag.Button(text, **kwargs)
    else:
        return fasttag.Button(text, onclick=onclick, **kwargs)

def Input(onchange=None, **kwargs):
    if callable(onchange):
        onchange = post_method_creator(global_app)(onchange)
    if isinstance(onchange, URLM):
        kwargs[f"hx_{onchange.__method__}"] = str(onchange)
        return fasttag.Input(**kwargs)
    elif not onchange:
        return fasttag.Input(**kwargs)
    else:
        return fasttag.Input(onchange=onchange, **kwargs)


def Form(*args, onsubmit=None, **kwargs):
    if callable(onsubmit):
        onsubmit = post_method_creator(global_app)(onsubmit)
    if isinstance(onsubmit, URLM):
        kwargs[f"hx_{onsubmit.__method__}"] = str(onsubmit)
        return fasttag.Form(*args, **kwargs)
    elif not onsubmit:
        return fasttag.Form(*args, **kwargs)
    else:
        return fasttag.Form(*args, onsubmit=onsubmit, **kwargs)


def rsql_html_app(live=True, debug=True, hdrs=static_hdrs, default_hdrs=False, **kwargs):
    app,rt = fast_app(live=live, debug=debug, hdrs=hdrs, default_hdrs=default_hdrs, **kwargs)
    rtx = rt_with_sqlx(rt, app)
    return app,rtx
