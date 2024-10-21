import time, asyncio
from .helpers import *
from fasttag import *
import fasttag
from rsql import URLM, Database
import threading
from queue import Queue
import uvicorn
from uvicorn.protocols.http.h11_impl import H11Protocol
from collections import defaultdict
import contextvars
import rsql
from functools import lru_cache as memoize
HTMXWS = True

tab_id = contextvars.ContextVar('tab_id', default=0)
accept_port = contextvars.ContextVar('accept_port', default=None)
last_tab_id = 1

# Replace the global queues dict with a thread-safe defaultdict
from collections import defaultdict
queues = defaultdict(Queue)
tab_ids_by_port = defaultdict(list)
objects_per_tab = defaultdict(list)
routes_per_tab = defaultdict(list)
destructors_per_tab = defaultdict(list)
sends = {}
import asyncio
def send_event(target_tab_id, event):
    # print("send_event", target_tab_id, tab_id.get(), event, sends)
    if target_tab_id == tab_id.get():
        queues[target_tab_id].put(event)
    elif target_tab_id in sends:
        # print("sending to ws", target_tab_id)
        asyncio.run(sends[target_tab_id](event))
    else:
        queues[target_tab_id].put(event)

def rt_with_sqlx(rt, app):
    def rtx(route):
        def decorator(func):
            return rt(route)(with_sqlx(func, app))
        return decorator
    return rtx

from inspect import isfunction,ismethod,Parameter,get_annotations
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
        if isinstance(result, rsql.Value):
            result = value(result)
        if isinstance(result, tuple):
            t = []
            for r in result:
                if isinstance(r, rsql.Value):
                    t.append(value(r))
                else:
                    t.append(r)
            result = tuple(t)
        q = list(queues[tab_id.get()].queue)
        queues[tab_id.get()] = Queue()
        
        end_time = time.time()
        render_time = (end_time - start_time) * 1000  # Convert to milliseconds
        # print("reading queue", tab_id, f"Rendering time: {render_time:.2f} ms", "hx_request", hx_request, "read queue", q)
        
        if not hx_request:
            q.append(Script(f"document.body.addEventListener('htmx:configRequest', function(evt) {{ evt.detail.headers['SQLX-Tab-Id'] = '{last_tab_id}';}});"))
            q.append(Meta(name="htmx-config", content='{"defaultSwapStyle":"none"}'))
            if HTMXWS:
                q.append(Div(hx_ext="ws", ws_connect=f"/htmxws/{last_tab_id}"))
            last_tab_id += 1
        q.append(HttpHeader("Server-Timing", f"rsql.html;dur={render_time:.2f}"))
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

def table(t, cb=None, header=None, id=None, tab_id0=None, infinite=False, next_button=False):
    if not id:
        id = nextid()
    if not cb:
        cb = lambda x: tuple([Td(x[col]) for col in t.columns])
    if not header:
        header = tuple([Th(col) for col in t.columns])
    r = fasttag.Table(
        *([Thead(header)] if header else []),
        Tbody(*[Tr(cb(row), id=f"e{abs(row.__hash__())}") for row in t], id=id))
    if tab_id0:
        tid = tab_id0.get()
    else:
        tid = tab_id.get()
    print(f"table, tab_id0={tab_id0}, tid={tid}")
    objects_per_tab[tid].append(t)
    if type(t) == rsql.Sort:
        destructors_per_tab[tid].append(
            t.on_insert(lambda index, row: send_event(tid, Template(Tbody(Tr(cb(row), id=f"e{abs(row.__hash__())}"),
                                    hx_swap_oob=(f"afterend: #{id} > :nth-child({index})" if index else f"beforeend: #{id}"))))))
        destructors_per_tab[tid].append(
            t.on_update(lambda index, old, new: send_event(tid, Template(Tr(cb(new),id=f"e{abs(new.__hash__())}", 
                                                                     hx_swap_oob=f"outerHTML: #{id} > :nth-child({index+1})")))))
        destructors_per_tab[tid].append(
            t.on_delete(lambda index, row: send_event(tid, Template(hx_swap_oob=f"delete: #{id} > :nth-child({index+1})"))))
        destructors_per_tab[tid].append(
            t.on_reset(lambda: send_event(tid, Template(Tbody(*[Tr(cb(row), id=f"e{abs(row.__hash__())}") for row in t], hx_swap_oob=f"innerHTML: #{id}")))))
        if next_button:
            r = r + Button("Next", onclick=lambda: t.set_limit(t.limit+50))
        if infinite:
            load_more = post_method_creator(global_app)(lambda: t.set_limit(t.limit+50))
            r = r + Script(f"var {id}_height = document.getElementById('{id}').clientHeight/2; var {id}_loading=false; document.addEventListener('scroll', function(evt) {{ if (!{id}_loading) if(window.scrollY+window.innerHeight > document.getElementById('{id}').clientHeight + document.getElementById('{id}').scrollTop - {id}_height) {{ console.log('load more'); {id}_loading = true; htmx.ajax('POST', '{load_more}', {{target: '#{id}'}}).then(function(data) {{console.log('loaded', data); {id}_loading = false;}});}};}})")
    else:
        destructors_per_tab[tid].append(
            t.on_insert(lambda row: send_event(tid, Template(Tbody(Tr(cb(row), id=f"e{abs(row.__hash__())}"), hx_swap_oob=f"beforeend:#{id}")))))
        destructors_per_tab[tid].append(
            t.on_delete(lambda row: send_event(tid, Template(Tr(id=f"e{abs(row.__hash__())}", hx_swap_oob="delete")))))
        destructors_per_tab[tid].append(
            t.on_update(lambda old, new: send_event(tid, Template(Tr(cb(new),id=f"e{abs(new.__hash__())}", hx_swap_oob=f"outerHTML: #e{abs(old.__hash__())}")))))
        destructors_per_tab[tid].append(
            t.on_reset(lambda: send_event(tid, Template(Tbody(*[Tr(cb(row), id=f"e{abs(row.__hash__())}") for row in t], hx_swap_oob=f"innerHTML: #{id}")))))
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
        t.on_insert(lambda row: send_event(tid, Li(cb(row), id=f"e{abs(row.__hash__())}", hx_swap_oob=f"beforeend: #{id}")))
    )
    destructors_per_tab[tid].append(
        t.on_delete(lambda row: send_event(tid, Li(id=f"e{abs(row.__hash__())}", hx_swap_oob="delete")))
    )
    destructors_per_tab[tid].append(
        t.on_update(lambda old, new: send_event(tid, Li(cb(new), id=f"e{abs(new.__hash__())}", hx_swap_oob=f"outerHTML: #e{abs(old.__hash__())}")) or 
            send_event(tid, Script(f"console.log('update {old} {new}');"))
        )
    )
    return r

def value(v, tab_id0=None):
    if isinstance(v, rsql.View):
        return table(v, tab_id0=tab_id0)
    id = nextid()
    if tab_id0:
        tid = tab_id0.get()
    else:
        tid = tab_id.get()
    v.onchange(lambda new: send_event(tid, Span(new, id=id, hx_swap_oob=f"true")))
    objects_per_tab[tid].append(v)
    return Span(v.value, id=id)

def show_if(cond, *args):
    global objects_per_tab
    id = nextid()
    tid = tab_id.get()
    objects_per_tab[tid].append(cond)
    cond.onchange(lambda value: send_event(tid, Span(args, id=id, hx_swap_oob="true", style=None if value else "display: none;")))
    return Span(args, id=id, style=None if cond.value else "display: none;")

def show_unless(cond, *args):
    return show_if(cond.map(lambda x: not x), *args)

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
        print("post_method_creator registering", url)
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


class ServerTimingMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        t = time.time()
        async def _send(message) -> None:
            if message['type'] == 'http.response.start':
                message['headers'].append((b'Server-Timing', f"fasthtml;dur={1000*(time.time() - t):.2f}".encode()))
            await send(message)
        await self.app(scope, receive, _send)

import asyncio
def rsql_html_app(live=True, debug=True, db=None, hdrs=static_hdrs, default_hdrs=False, before=None, pico=False, **kwargs):
    if pico:
        hdrs.append(picocss)
    app,rt = fast_app(live=live, debug=debug, hdrs=hdrs, default_hdrs=default_hdrs, before=before, middleware=[Middleware(ServerTimingMiddleware)], **kwargs)
    rtx = rt_with_sqlx(rt, app)

    if db:
        register_tables(rtx, db)
        tab_id0 = tab_id
        db.tohtml = lambda v: value(v, tab_id0)

    async def on_conn(ws, send):
        tid = int(ws.url.path.split('/')[-1])
        sends[tid] = send

    async def on_disconn(ws, send):
        tid = int(ws.url.path.split('/')[-1])
        if tid in sends:
            del sends[tid]
    if HTMXWS:
        @app.ws('/htmxws/{tid}', conn=on_conn, disconn=on_disconn)
        async def on_message(send):
            pass
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
        accept_port.set(self.client[1])
        start_time = time.time()
        super().data_received(data)
        end_time = time.time()
        processing_time = (end_time - start_time) * 1000  # Convert to milliseconds
        server_timing_header = f"server-total;dur={processing_time:.2f}"
    
import inspect

reqs = 0
def print_memory_thread():
    secs = 0
    global reqs
    while True:
        time.sleep(1)
        secs += 1
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        print(f"Used memory: {memory_info.rss / 1024 / 1024:.2f} MB, reqs: {reqs}, secs: {secs}, rps: {reqs/secs}")

def simple_load_test_thread(url='/'):
    global reqs
    import socket
    import time
    time.sleep(0.1)
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(('localhost', 5001))
            s.sendall(b'GET '+url.encode('utf-8')+b' HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n')
            response = s.recv(1024)
            s.close()
            reqs += 1
        except Exception as e:
            print(f"Error calling server: {e}")
            raise e

# Use log_level="critical" to suppress logging
def serve(appname=None, app='app', port=5001, reload=True, log_level="info", host="0.0.0.0",
                    timeout_keep_alive=600, print_memory=False, simple_load_test=None, **argv):
    print("rsql_html:serve starting server on http://" + host + ":" + str(port), appname, app)
    print("Reload is set to:", reload)
    
    # __file__ is the path to the current file
    bk = inspect.currentframe().f_back
    glb = bk.f_globals
    code = bk.f_code

    if not appname:
            if glb.get('__name__')=='__main__': appname = Path(glb.get('__file__', '')).stem
            elif code.co_name=='main' and bk.f_back.f_globals.get('__name__')=='__main__': appname = inspect.getmodule(bk).__name__
    if appname:
        if not port: port=int(os.getenv("PORT", default=5001))
        print(f'Link: http://{"localhost" if host=="0.0.0.0" else host}:{port}')

    if print_memory:
        threading.Thread(target=print_memory_thread, daemon=True).start()
    if simple_load_test:
        threading.Thread(target=simple_load_test_thread, args=(simple_load_test,), daemon=True).start()
    
    print(f"rsql_html: serve starting server on http://{host}:{port} for {appname}:{app}")

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
    print("rsql_html: serve server stopped")

