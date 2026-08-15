#!/usr/bin/env python3
"""
gh.py - tiny GitHub Contents-API client used by pool_connector.py and pearl_miner.py.

Only the standard library is used, so the scripts run on a bare Python 3.8+ install
on any mining rig (Windows or Linux) with no `pip install` step.

The Contents API gives us three things we need:
  * read  a file + its blob sha            -> get_file()
  * write a file with optimistic locking   -> put_file(..., sha=...)
  * atomic read-modify-write with retries  -> append_lines() / take_and_clear()

Optimistic locking matters because several mining PCs may append shares to the same
shares.txt at the same time; GitHub answers 409 when the sha we sent is stale, and we
simply re-read and retry instead of losing a share.
"""

import base64
import json
import os
import random
import time
import urllib.error
import urllib.request

API = "https://api.github.com"


class GitHubFileStore:
    def __init__(self, token, owner, repo, branch="main", author_email=None, author_name=None):
        self.token = token
        self.owner = owner
        self.repo = repo
        self.branch = branch
        self.author_email = author_email or "bot@example.com"
        self.author_name = author_name or "prl-pool-bridge"

    # ---------------------------------------------------------------- internals
    def _request(self, method, path, payload=None, params=None):
        url = "%s%s" % (API, path)
        if params:
            url += "?" + "&".join("%s=%s" % (k, urllib.parse.quote(str(v))) for k, v in params.items())
        data = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", "token " + self.token)
        req.add_header("Accept", "application/vnd.github+json")
        req.add_header("User-Agent", "prl-pool-bridge")
        if data:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read()
                return resp.status, (json.loads(body) if body else {})
        except urllib.error.HTTPError as e:
            body = e.read()
            try:
                return e.code, json.loads(body)
            except Exception:
                return e.code, {"message": body.decode("utf-8", "replace")}

    def _content_path(self, path):
        return "/repos/%s/%s/contents/%s" % (self.owner, self.repo, path)

    # ------------------------------------------------------------------- public
    def get_file(self, path):
        """Return (text, sha). text is None when the file does not exist yet."""
        status, body = self._request("GET", self._content_path(path), params={"ref": self.branch})
        if status == 404:
            return None, None
        if status != 200:
            raise RuntimeError("GET %s failed: %s %s" % (path, status, body.get("message")))
        raw = base64.b64decode(body.get("content", "")) if body.get("encoding") == "base64" else b""
        return raw.decode("utf-8", "replace"), body.get("sha")

    def get_file_raw(self, path):
        """Cheap read straight from raw.githubusercontent (no API rate cost, may lag a few s)."""
        url = "https://raw.githubusercontent.com/%s/%s/%s/%s" % (self.owner, self.repo, self.branch, path)
        req = urllib.request.Request(url)
        req.add_header("Authorization", "token " + self.token)
        req.add_header("User-Agent", "prl-pool-bridge")
        req.add_header("Cache-Control", "no-cache")
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                return resp.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise

    def put_file(self, path, text, message, sha=None):
        """Create/overwrite a file. Returns the new blob sha, or None on a 409 conflict."""
        payload = {
            "message": message,
            "content": base64.b64encode(text.encode("utf-8")).decode("ascii"),
            "branch": self.branch,
            "committer": {"name": self.author_name, "email": self.author_email},
        }
        if sha:
            payload["sha"] = sha
        status, body = self._request("PUT", self._content_path(path), payload)
        if status in (200, 201):
            return body["content"]["sha"]
        if status in (409, 422):
            return None  # stale sha -> caller retries
        raise RuntimeError("PUT %s failed: %s %s" % (path, status, body.get("message")))

    def overwrite(self, path, text, message, retries=6):
        """Blind overwrite (used for jobs.txt, which only the connector writes)."""
        for attempt in range(retries):
            _, sha = self.get_file(path)
            new_sha = self.put_file(path, text, message, sha)
            if new_sha:
                return new_sha
            time.sleep(0.4 * (attempt + 1) + random.random() * 0.3)
        raise RuntimeError("overwrite %s: gave up after %d retries" % (path, retries))

    def append_lines(self, path, lines, message, retries=8):
        """Atomically append lines to a file (used for shares.txt by every miner PC)."""
        chunk = "".join(l if l.endswith("\n") else l + "\n" for l in lines)
        for attempt in range(retries):
            current, sha = self.get_file(path)
            base = current if current is not None else ""
            if base and not base.endswith("\n"):
                base += "\n"
            new_sha = self.put_file(path, base + chunk, message, sha)
            if new_sha:
                return new_sha
            time.sleep(0.4 * (attempt + 1) + random.random() * 0.5)
        raise RuntimeError("append %s: gave up after %d retries" % (path, retries))

    def take_and_clear(self, path, message, retries=8):
        """Read a file and clear it in one optimistic transaction.

        Returns the list of non-empty lines that were consumed. If another PC wrote
        to the file between our read and our write, GitHub rejects the stale sha and
        we retry, so no share is ever consumed twice or silently dropped."""
        for attempt in range(retries):
            current, sha = self.get_file(path)
            if current is None:
                return []
            lines = [l for l in current.splitlines() if l.strip()]
            if not lines:
                return []
            new_sha = self.put_file(path, "", message, sha)
            if new_sha:
                return lines
            time.sleep(0.4 * (attempt + 1) + random.random() * 0.5)
        raise RuntimeError("take_and_clear %s: gave up after %d retries" % (path, retries))

    def ensure_file(self, path, initial_text=""):
        text, _ = self.get_file(path)
        if text is None:
            self.put_file(path, initial_text, "init %s" % path)
            return True
        return False


def load_config(path=None):
    """Load config.json, then overlay secrets that are deliberately kept out of git.

    Precedence (highest first):
      1. $GH_TOKEN
      2. config.local.json  (gitignored, sits next to config.json)
      3. config.json
    GitHub push protection rejects any commit containing a PAT, so the token must
    never live in the tracked config."""
    base = os.path.dirname(os.path.abspath(__file__))
    path = path or os.path.join(base, "config.json")
    with open(path, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)

    local = os.path.join(os.path.dirname(os.path.abspath(path)), "config.local.json")
    if os.path.exists(local):
        with open(local, "r", encoding="utf-8") as fh:
            overlay = json.load(fh)
        for section, values in overlay.items():
            if isinstance(values, dict):
                cfg.setdefault(section, {}).update(values)
            else:
                cfg[section] = values

    cfg["github"]["token"] = os.environ.get("GH_TOKEN") or cfg["github"].get("token", "")
    if not cfg["github"]["token"]:
        raise SystemExit(
            "No GitHub token. Set GH_TOKEN, or put it in config.local.json:\n"
            '  {"github": {"token": "ghp_..."}}')
    return cfg


def store_from_config(cfg):
    g = cfg["github"]
    return GitHubFileStore(g["token"], g["owner"], g["repo"], g.get("branch", "main"), g.get("email"))


import urllib.parse  # noqa: E402  (kept last: only needed by _request's param encoding)
