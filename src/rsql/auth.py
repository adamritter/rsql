# Usage:

# Setup authentication: https://docs.fastht.ml/explains/oauth.html
# import rsql.auth
# app, rtx = rsql_html_app(before=rsql.auth.before())
# rsql.auth.setup(app)
# ...
# @rtx('/')
# def get(auth):
#     return "Hello, " + auth

from fasthtml.common import *
from fasthtml.oauth import GitHubAppClient
import os

auth_callback_path = "/auth_redirect"

def before_fn(req, session):
    auth = req.scope['auth'] = session.get('user_id', None)
    if not auth: return RedirectResponse('/login', status_code=303)
def before(skip=[]):
    return Beforeware(before_fn, skip=['/login', auth_callback_path, *skip])

def setup(app, base_url="http://localhost:5001"):
    github_client = GitHubAppClient(
        os.getenv("GITHUB_CLIENT_ID"),
        os.getenv("GITHUB_CLIENT_SECRET"),
        base_url + auth_callback_path
    )
    @app.get('/login')
    def login(request):
        login_link = github_client.login_link(auth_callback_path)
        return P(A('Login with GitHub', href=login_link))    

    @app.get('/logout')
    def logout(request):
        session = request.scope['session']
        session.pop('user_id', None)
        return RedirectResponse('/', status_code=303)

    # User comes back to us with an auth code from Github
    @app.get(auth_callback_path)
    def auth_redirect(code:str, request, session):
        redir = "auth_redirect"
        user_info = github_client.retr_info(code)
        print(user_info)
        user_id = user_info[github_client.id_key] # get their ID
        session['user_id'] = user_id # save ID in the session
        # create a db entry for the user
        return RedirectResponse('/', status_code=303)
