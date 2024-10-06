import time
from .helpers import *
from fasttag import *
import fasttag
from rsql import URLM
import threading
from queue import Queue
import uvicorn
from uvicorn.protocols.http.h11_impl import H11Protocol
from collections import defaultdict

# Replace global variables with thread-local storage
import contextvars

tab_id = contextvars.ContextVar('tab_id', default=0)
accept_port = contextvars.ContextVar('accept_port', default=None)
last_tab_id = 0

# Replace the global queues dict with a thread-safe defaultdict
from collections import defaultdict
queues = defaultdict(Queue)
tab_ids_by_port = defaultdict(list)
objects_per_tab = defaultdict(list)
routes_per_tab = defaultdict(list)
destructors_per_tab = defaultdict(list)


def append_queue(e):
    queues[tab_id.get()].put(e)

def get_and_clear_queue():
    q = list(queues[tab_id.get()].queue)
    queues[tab_id.get()] = Queue()
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
            tab_id.set(sqlx_tab_id)
        else:
            tab_id.set(last_tab_id)
            tab_ids_by_port[accept_port.get()].append(last_tab_id)
        
        # print("args", args, "hx_request", hx_request, "sqlx_tab_id", sqlx_tab_id, "tab_id", tab_id, "app", app, "accept_port", accept_port)
        global global_app
        global_app = app
        result = f(*args)

        q = list(queues[tab_id.get()].queue)
        queues[tab_id.get()] = Queue()
        
        end_time = time.time()
        render_time = (end_time - start_time) * 1000  # Convert to milliseconds
        # print("reading queue", tab_id, f"Rendering time: {render_time:.2f} ms", "hx_request", hx_request, "read queue", q)
        
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
    r = fasttag.Table(
        Thead(header) if header else None,
         Tbody(*[Tr(cb(row), id=f"e{abs(row.__hash__())}") for row in t], id=id))
    tid = tab_id.get()
    objects_per_tab[tid].append(t)
    destructors_per_tab[tid].append(
        t.on_insert(lambda row: append_queue_to(tid, Template(Tbody(Tr(cb(row), id=f"e{abs(row.__hash__())}"), hx_swap_oob=f"beforeend:#{id}")))))
    destructors_per_tab[tid].append(
        t.on_delete(lambda row: append_queue_to(tid, Template(Tr(id=f"e{abs(row.__hash__())}", hx_swap_oob="delete")))))
    destructors_per_tab[tid].append(
        t.on_update(lambda old, new: append_queue_to(tid, Template(Tr(cb(new),id=f"e{abs(new.__hash__())}", hx_swap_oob=f"outerHTML: #e{abs(old.__hash__())}")))))
    return r

def ulli(t, cb, header=None, id=None):
    if not id:
        id = nextid()
    r = fasttag.Ul(
        Li(header) if header else None,
        *[Li(cb(row), id=f"e{abs(row.__hash__())}") for row in t],
        id=id
    )
    tid = tab_id.get()
    objects_per_tab[tid].append(t)  
    destructors_per_tab[tid].append(
        t.on_insert(lambda row: append_queue_to(tid, Li(cb(row), id=f"e{abs(row.__hash__())}", hx_swap_oob=f"beforeend: #{id}")))
    )
    destructors_per_tab[tid].append(
        t.on_delete(lambda row: append_queue_to(tid, Li(id=f"e{abs(row.__hash__())}", hx_swap_oob="delete")))
    )
    destructors_per_tab[tid].append(
        t.on_update(lambda old, new: append_queue_to(tid, Li(cb(new), id=f"e{abs(new.__hash__())}", hx_swap_oob=f"outerHTML: #e{abs(old.__hash__())}")) or 
            append_queue_to(tid, Script(f"console.log('update {old} {new}');"))
        )
    )
    return r

def value(v):
    id = nextid()
    tid = tab_id.get()
    v.onchange(lambda new: append_queue_to(tid, Span(new, id=id, hx_swap_oob=f"true")))
    return Span(v.value, id=id)

def show_if(cond, *args):
    global objects_per_tab
    id = nextid()
    tid = tab_id.get()
    objects_per_tab[tid].append(cond)
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
        rr=None
        for r in app.routes:
            if r.path == url:
                rr=r
        if not rr:
            raise Exception("route not found")
        routes_per_tab[tab_id.get()].append(rr)
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

class LoggingProtocol(H11Protocol):
    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        # print(f"Connection accepted from {peername}")
        super().connection_made(transport)

    def connection_lost(self, exc):
        super().connection_lost(exc)
        # print("Connection closed from", self.client, ", closing ", len(tab_ids_by_port[self.client[1]]), "tabs")
        for tid in tab_ids_by_port[self.client[1]]:
            # print("closing tab", tid, "because connection closed", self.client)
            if tid in queues:
                del queues[tid]
            if tid in objects_per_tab:
                del objects_per_tab[tid]
            if tid in routes_per_tab:
                for r in routes_per_tab[tid]:
                    global_app.routes.remove(r)
                del routes_per_tab[tid]
            if tid in destructors_per_tab:
                for d in destructors_per_tab[tid]:
                    d()
                del destructors_per_tab[tid]
        del tab_ids_by_port[self.client[1]]
    
    def data_received(self, data):
        # print("Data received from", self.client)
        accept_port.set(self.client[1])
        super().data_received(data)

import inspect

# Use log_level="critical" to suppress logging
def rsql_html_serve(appname=None, app='app', port=5001, reload=True, log_level="info", host="0.0.0.0", timeout_keep_alive=600, **argv):
    print("rsql_html_serve starting server on http://" + host + ":" + str(port))
    print("Reload is set to:", reload)
    
    # __file__ is the path to the current file
    print("Current file path:", __file__)
    bk = inspect.currentframe().f_back
    glb = bk.f_globals
    code = bk.f_code
    appname = None

    if not appname:
            if glb.get('__name__')=='__main__': appname = Path(glb.get('__file__', '')).stem
            elif code.co_name=='main' and bk.f_back.f_globals.get('__name__')=='__main__': appname = inspect.getmodule(bk).__name__
    if appname:
        if not port: port=int(os.getenv("PORT", default=5001))
        print(f'Link: http://{"localhost" if host=="0.0.0.0" else host}:{port}')

    uvicorn.run(
        f'{appname}:{app}',
        host=host,
        port=port,
        http=LoggingProtocol,
        timeout_keep_alive=timeout_keep_alive,
        log_level=log_level,
        reload=reload,
        workers=1,  # Single worker for reloading
        **argv
    )
    print("rsql_html_serve server stopped")