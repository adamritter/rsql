from rsql.html import *
import requests, logging, threading, time
from websockets.sync.client import connect

logging.basicConfig(level=logging.DEBUG)
s = requests.Session()

db = rsql.Database(":memory:")
t = db.table("a", b=int)
t.insert(b=1)
# reset
rsql.html.lastid = 0
objects_per_tab.clear()
queues.clear()
app, rtx = rsql_html_app(db=db, live=False, debug=False)
@rtx('/')
def get():
    return t.only()

@rtx('/update')
def post():
    t.update({}, b=2)

def update_test():
    server_thread = threading.Thread(target=serve, daemon=True, kwargs={"appname": "html_test", "port": 5674, "reload": False})
    server_thread.start()
    time.sleep(0.01)
    html = s.get("http://localhost:5674/").text
    assert("""<span id="e1">&lt;Row a {'id': 1, 'b': 1}></span>""" in html)
    ws = connect("ws://localhost:5674/htmxws/1")
    s.post("http://localhost:5674/update")
    msg = ws.recv()
    assert("""<span id="e1" hx-swap-oob="true">&lt;Row a {'id': 1, 'b': 2}></span>""" in msg)
    ws.close()

if __name__ == "__main__":  
    update_test()
