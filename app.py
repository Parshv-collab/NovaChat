"""
NovaChat - A real-time messaging web application inspired by the layout and
feature set of popular chat apps, built entirely from original code.

Single-file Flask + Flask-SocketIO + MongoDB application.

Run with:
    pip install flask flask-socketio pymongo bcrypt
    (make sure MongoDB is running on localhost:27017, or set MONGO_URI)
    python app.py
"""

import os
import secrets
import datetime
from functools import wraps

from flask import Flask, request, jsonify, session, redirect, url_for, render_template_string
from flask_socketio import SocketIO, emit, join_room, leave_room
from pymongo import MongoClient, DESCENDING
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from bson import ObjectId
from bson.errors import InvalidId
import bcrypt
from dotenv import load_dotenv

# --------------------------------------------------------------------------
# MongoDB connection handling
# --------------------------------------------------------------------------

load_dotenv()

MONGO_URI = os.environ.get("MONGO_URI")
if not MONGO_URI:
    print("ERROR: MONGO_URI environment variable not set!")
    raise SystemExit(1)

try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_client.server_info()  # forces connection check
    db = mongo_client["novachat_db"]
    users_col = db["users"]
    chats_col = db["chats"]
    messages_col = db["messages"]

    # Indexes for performance / uniqueness
    users_col.create_index("username", unique=True)
    chats_col.create_index("participants")
    messages_col.create_index([("chat_id", 1), ("timestamp", 1)])
    print("[NovaChat] Connected to MongoDB successfully.")
except ConnectionFailure as exc:
    print(f"[NovaChat] FATAL: Could not connect to MongoDB at {MONGO_URI}: {exc}")
    raise SystemExit(1)

# --------------------------------------------------------------------------
# Flask + SocketIO setup
# --------------------------------------------------------------------------
app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", secrets.token_hex(32))

# Use threading mode for better Windows compatibility
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode="threading",
    ping_timeout=60,
    ping_interval=25
)

# Maps username -> set of socket ids (a user may have multiple tabs open)
online_sockets = {}

# Store active calls
active_calls = {}  # chat_id -> {caller, callee, status}

# --------------------------------------------------------------------------
# Helper functions
# --------------------------------------------------------------------------
def login_required(view_func):
    """Decorator that ensures a valid session exists before hitting a route."""
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if "username" not in session:
            if request.path.startswith("/api/"):
                return jsonify({"error": "Not authenticated"}), 401
            return redirect(url_for("login_page"))
        return view_func(*args, **kwargs)
    return wrapped


def now_iso():
    return datetime.datetime.utcnow().isoformat() + "Z"


def to_oid(id_str):
    try:
        return ObjectId(id_str)
    except (InvalidId, TypeError):
        return None


def serialize_user(user_doc, private=False):
    """Convert a Mongo user document into a JSON-safe dict."""
    if not user_doc:
        return None
    out = {
        "username": user_doc.get("username"),
        "avatar": user_doc.get("avatar") or "",
        "about": user_doc.get("about", "Available"),
        "online": user_doc.get("online", False),
        "last_seen": user_doc.get("last_seen"),
    }
    if private:
        out["created_at"] = user_doc.get("created_at")
    return out


def serialize_message(msg, requester):
    """Convert a Mongo message document into a JSON-safe dict, respecting
    per-user 'delete for me' visibility."""
    deleted_for = msg.get("deleted_for", [])
    is_hidden = requester in deleted_for
    
    # Handle voice message data
    voice_data = msg.get("voice_data")
    if voice_data and is_hidden:
        voice_data = None
    
    return {
        "id": str(msg["_id"]),
        "chat_id": str(msg["chat_id"]),
        "sender": msg["sender"],
        "text": "" if (is_hidden or msg.get("deleted_for_everyone")) else msg.get("text", ""),
        "timestamp": msg["timestamp"],
        "read_by": msg.get("read_by", []),
        "delivered_to": msg.get("delivered_to", []),
        "edited": msg.get("edited", False),
        "deleted_for_everyone": msg.get("deleted_for_everyone", False),
        "hidden_for_me": is_hidden,
        "starred_by": msg.get("starred_by", []),
        "reply_to": msg.get("reply_to"),
        "reply_preview": msg.get("reply_preview"),
        "forwarded": msg.get("forwarded", False),
        "is_voice": msg.get("is_voice", False),
        "voice_data": voice_data if not is_hidden else None,
        "voice_duration": msg.get("voice_duration", 0),
    }


def serialize_chat(chat, requester):
    """Convert a Mongo chat document into a JSON-safe dict, resolving the
    'other user' for direct chats and unread counts for the requester."""
    chat_id_str = str(chat["_id"])
    unread = messages_col.count_documents({
        "chat_id": chat["_id"],
        "sender": {"$ne": requester},
        "read_by": {"$ne": requester},
        "deleted_for": {"$ne": requester},
    })

    result = {
        "id": chat_id_str,
        "is_group": chat.get("is_group", False),
        "last_message": chat.get("last_message"),
        "updated_at": chat.get("updated_at"),
        "unread_count": unread,
        "pinned": requester in chat.get("pinned_by", []),
        "archived": requester in chat.get("archived_by", []),
        "muted": requester in chat.get("muted_by", []),
    }

    if chat.get("is_group"):
        result["name"] = chat.get("group_name", "Group")
        result["avatar"] = chat.get("group_icon", "")
        result["description"] = chat.get("group_description", "")
        result["members"] = chat.get("participants", [])
        result["admins"] = chat.get("group_admins", [])
        result["online"] = False
    else:
        other_username = next((p for p in chat["participants"] if p != requester), requester)
        other_user = users_col.find_one({"username": other_username})
        result["name"] = other_username
        result["avatar"] = other_user.get("avatar", "") if other_user else ""
        result["online"] = other_user.get("online", False) if other_user else False
        result["last_seen"] = other_user.get("last_seen") if other_user else None
        result["participants"] = chat["participants"]

    return result


def get_or_create_direct_chat(user_a, user_b):
    """Find an existing 1:1 chat between two users, or create one."""
    existing = chats_col.find_one({
        "is_group": False,
        "participants": {"$all": [user_a, user_b], "$size": 2},
    })
    if existing:
        return existing

    new_chat = {
        "participants": [user_a, user_b],
        "is_group": False,
        "last_message": None,
        "updated_at": now_iso(),
        "pinned_by": [],
        "archived_by": [],
        "muted_by": [],
    }
    result = chats_col.insert_one(new_chat)
    new_chat["_id"] = result.inserted_id
    return new_chat


def user_can_access_chat(username, chat):
    return chat is not None and username in chat.get("participants", [])


# --------------------------------------------------------------------------
# Auth routes
# --------------------------------------------------------------------------
@app.route("/")
def index():
    if "username" in session:
        return redirect(url_for("chat_page"))
    return redirect(url_for("login_page"))


@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "GET":
        if "username" in session:
            return redirect(url_for("chat_page"))
        return render_template_string(AUTH_TEMPLATE, mode="login")

    data = request.get_json(silent=True) or request.form
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""

    if not username or not password:
        return jsonify({"error": "Username and password are required."}), 400

    user = users_col.find_one({"username": username})
    if not user or not bcrypt.checkpw(password.encode("utf-8"), user["password_hash"]):
        return jsonify({"error": "Invalid username or password."}), 401

    session["username"] = username
    users_col.update_one({"username": username}, {"$set": {"online": True}})
    return jsonify({"success": True, "redirect": url_for("chat_page")})


@app.route("/register", methods=["GET", "POST"])
def register_page():
    if request.method == "GET":
        if "username" in session:
            return redirect(url_for("chat_page"))
        return render_template_string(AUTH_TEMPLATE, mode="register")

    data = request.get_json(silent=True) or request.form
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""

    if not username or not password:
        return jsonify({"error": "Username and password are required."}), 400
    if len(username) < 3:
        return jsonify({"error": "Username must be at least 3 characters."}), 400
    if len(password) < 4:
        return jsonify({"error": "Password must be at least 4 characters."}), 400

    password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())

    new_user = {
        "username": username,
        "password_hash": password_hash,
        "avatar": "",
        "about": "Hey there! I am using NovaChat.",
        "last_seen": now_iso(),
        "online": True,
        "created_at": now_iso(),
    }

    try:
        users_col.insert_one(new_user)
    except DuplicateKeyError:
        return jsonify({"error": "That username is already taken."}), 409

    session["username"] = username
    return jsonify({"success": True, "redirect": url_for("chat_page")})


@app.route("/logout")
def logout_page():
    username = session.get("username")
    if username:
        users_col.update_one(
            {"username": username},
            {"$set": {"online": False, "last_seen": now_iso()}},
        )
        socketio.emit("online_status", {"username": username, "online": False, "last_seen": now_iso()})
    session.clear()
    return redirect(url_for("login_page"))


@app.route("/chat")
@login_required
def chat_page():
    return render_template_string(CHAT_TEMPLATE, username=session["username"])


# --------------------------------------------------------------------------
# Chat / message API routes
# --------------------------------------------------------------------------
@app.route("/api/chats", methods=["GET"])
@login_required
def api_list_chats():
    username = session["username"]
    chats = list(chats_col.find({"participants": username}))
    serialized = [serialize_chat(c, username) for c in chats]
    # Sort: pinned first, then most recently updated
    serialized.sort(key=lambda c: (not c["pinned"], c["updated_at"] or ""), reverse=False)
    serialized.sort(key=lambda c: c["updated_at"] or "", reverse=True)
    serialized.sort(key=lambda c: c["pinned"], reverse=True)
    return jsonify({"chats": serialized})


@app.route("/api/chats/create", methods=["POST"])
@login_required
def api_create_chat():
    username = session["username"]
    data = request.get_json(silent=True) or {}

    if data.get("is_group"):
        group_name = (data.get("group_name") or "").strip()
        members = data.get("members") or []
        if not group_name:
            return jsonify({"error": "Group name is required."}), 400
        participants = list(set(members + [username]))
        if len(participants) < 2:
            return jsonify({"error": "A group needs at least 2 members."}), 400

        new_chat = {
            "participants": participants,
            "is_group": True,
            "group_name": group_name,
            "group_icon": data.get("group_icon", ""),
            "group_description": data.get("group_description", ""),
            "group_admins": [username],
            "last_message": {"text": f"{username} created the group", "sender": "system", "timestamp": now_iso()},
            "updated_at": now_iso(),
            "pinned_by": [],
            "archived_by": [],
            "muted_by": [],
        }
        result = chats_col.insert_one(new_chat)
        new_chat["_id"] = result.inserted_id
        return jsonify({"chat": serialize_chat(new_chat, username)})

    other_username = (data.get("participant") or "").strip()
    if not other_username or other_username == username:
        return jsonify({"error": "Invalid participant."}), 400
    if not users_col.find_one({"username": other_username}):
        return jsonify({"error": "User not found."}), 404

    chat = get_or_create_direct_chat(username, other_username)
    return jsonify({"chat": serialize_chat(chat, username)})


@app.route("/api/chats/<chat_id>/group/add", methods=["POST"])
@login_required
def api_group_add_member(chat_id):
    username = session["username"]
    oid = to_oid(chat_id)
    chat = chats_col.find_one({"_id": oid}) if oid else None
    if not chat or not chat.get("is_group") or not user_can_access_chat(username, chat):
        return jsonify({"error": "Group not found."}), 404

    new_member = (request.get_json(silent=True) or {}).get("username", "").strip()
    if not users_col.find_one({"username": new_member}):
        return jsonify({"error": "User not found."}), 404

    chats_col.update_one({"_id": oid}, {"$addToSet": {"participants": new_member}})
    updated = chats_col.find_one({"_id": oid})
    socketio.emit("group_updated", {"chat_id": chat_id}, room=chat_id)
    return jsonify({"chat": serialize_chat(updated, username)})


@app.route("/api/chats/<chat_id>/group/remove", methods=["POST"])
@login_required
def api_group_remove_member(chat_id):
    username = session["username"]
    oid = to_oid(chat_id)
    chat = chats_col.find_one({"_id": oid}) if oid else None
    if not chat or not chat.get("is_group") or not user_can_access_chat(username, chat):
        return jsonify({"error": "Group not found."}), 404

    member = (request.get_json(silent=True) or {}).get("username", "").strip()
    chats_col.update_one({"_id": oid}, {"$pull": {"participants": member, "group_admins": member}})
    updated = chats_col.find_one({"_id": oid})
    socketio.emit("group_updated", {"chat_id": chat_id}, room=chat_id)
    return jsonify({"chat": serialize_chat(updated, username)})


def _toggle_chat_flag(chat_id, field):
    username = session["username"]
    oid = to_oid(chat_id)
    chat = chats_col.find_one({"_id": oid}) if oid else None
    if not chat or not user_can_access_chat(username, chat):
        return jsonify({"error": "Chat not found."}), 404

    if username in chat.get(field, []):
        chats_col.update_one({"_id": oid}, {"$pull": {field: username}})
        new_state = False
    else:
        chats_col.update_one({"_id": oid}, {"$addToSet": {field: username}})
        new_state = True
    return jsonify({"success": True, field: new_state})


@app.route("/api/chats/<chat_id>/pin", methods=["POST"])
@login_required
def api_pin_chat(chat_id):
    return _toggle_chat_flag(chat_id, "pinned_by")


@app.route("/api/chats/<chat_id>/archive", methods=["POST"])
@login_required
def api_archive_chat(chat_id):
    return _toggle_chat_flag(chat_id, "archived_by")


@app.route("/api/chats/<chat_id>/mute", methods=["POST"])
@login_required
def api_mute_chat(chat_id):
    return _toggle_chat_flag(chat_id, "muted_by")


@app.route("/api/chats/<chat_id>", methods=["DELETE"])
@login_required
def api_delete_chat(chat_id):
    username = session["username"]
    oid = to_oid(chat_id)
    chat = chats_col.find_one({"_id": oid}) if oid else None
    if not chat or not user_can_access_chat(username, chat):
        return jsonify({"error": "Chat not found."}), 404

    # "Delete chat" hides it for this user by marking all messages deleted-for-me
    messages_col.update_many({"chat_id": oid}, {"$addToSet": {"deleted_for": username}})
    chats_col.update_one({"_id": oid}, {"$addToSet": {"archived_by": username}})
    return jsonify({"success": True})


@app.route("/api/messages/<chat_id>", methods=["GET"])
@login_required
def api_get_messages(chat_id):
    username = session["username"]
    oid = to_oid(chat_id)
    chat = chats_col.find_one({"_id": oid}) if oid else None
    if not chat or not user_can_access_chat(username, chat):
        return jsonify({"error": "Chat not found."}), 404

    limit = min(int(request.args.get("limit", 30)), 100)
    before = request.args.get("before")  # ISO timestamp cursor for infinite scroll

    query = {"chat_id": oid}
    if before:
        query["timestamp"] = {"$lt": before}

    cursor = messages_col.find(query).sort("timestamp", DESCENDING).limit(limit)
    msgs = list(cursor)
    msgs.reverse()  # chronological order for rendering

    # Mark messages as delivered to the requester (they've now fetched them)
    messages_col.update_many(
        {"chat_id": oid, "sender": {"$ne": username}, "delivered_to": {"$ne": username}},
        {"$addToSet": {"delivered_to": username}},
    )

    serialized = [serialize_message(m, username) for m in msgs]
    return jsonify({"messages": serialized, "has_more": len(msgs) == limit})


def _persist_message(chat_oid, sender, text, reply_to=None, forwarded=False, is_voice=False, voice_data=None, voice_duration=0):
    """Shared logic for saving a message + updating the parent chat's preview."""
    reply_preview = None
    if reply_to:
        parent = messages_col.find_one({"_id": to_oid(reply_to)})
        if parent:
            reply_preview = {"sender": parent["sender"], "text": parent.get("text", "")[:80]}

    msg = {
        "chat_id": chat_oid,
        "sender": sender,
        "text": text or "",
        "timestamp": now_iso(),
        "read_by": [sender],
        "delivered_to": [sender],
        "edited": False,
        "deleted_for_everyone": False,
        "deleted_for": [],
        "starred_by": [],
        "reply_to": reply_to,
        "reply_preview": reply_preview,
        "forwarded": bool(forwarded),
        "is_voice": bool(is_voice),
        "voice_data": voice_data if is_voice else None,
        "voice_duration": voice_duration if is_voice else 0,
    }
    result = messages_col.insert_one(msg)
    msg["_id"] = result.inserted_id

    # Update chat preview
    preview_text = text if text else ("🎤 Voice message" if is_voice else "")
    chats_col.update_one(
        {"_id": chat_oid},
        {"$set": {
            "last_message": {"text": preview_text, "sender": sender, "timestamp": msg["timestamp"]},
            "updated_at": msg["timestamp"],
        }},
    )
    return msg


@app.route("/api/message/<message_id>/edit", methods=["POST"])
@login_required
def api_edit_message(message_id):
    username = session["username"]
    oid = to_oid(message_id)
    msg = messages_col.find_one({"_id": oid}) if oid else None
    if not msg or msg["sender"] != username:
        return jsonify({"error": "Message not found or not editable."}), 404

    new_text = (request.get_json(silent=True) or {}).get("text", "").strip()
    if not new_text:
        return jsonify({"error": "Text cannot be empty."}), 400

    messages_col.update_one({"_id": oid}, {"$set": {"text": new_text, "edited": True}})
    updated = messages_col.find_one({"_id": oid})
    socketio.emit("message_edited", {
        "chat_id": str(msg["chat_id"]), "message_id": message_id, "text": new_text,
    }, room=str(msg["chat_id"]))
    return jsonify({"message": serialize_message(updated, username)})


@app.route("/api/message/<message_id>/delete", methods=["POST"])
@login_required
def api_delete_message(message_id):
    username = session["username"]
    oid = to_oid(message_id)
    msg = messages_col.find_one({"_id": oid}) if oid else None
    if not msg:
        return jsonify({"error": "Message not found."}), 404

    mode = (request.get_json(silent=True) or {}).get("mode", "me")

    if mode == "everyone":
        if msg["sender"] != username:
            return jsonify({"error": "You can only delete your own messages for everyone."}), 403
        messages_col.update_one({"_id": oid}, {"$set": {"deleted_for_everyone": True, "text": "", "voice_data": None}})
        socketio.emit("message_deleted", {
            "chat_id": str(msg["chat_id"]), "message_id": message_id, "mode": "everyone",
        }, room=str(msg["chat_id"]))
    else:
        messages_col.update_one({"_id": oid}, {"$addToSet": {"deleted_for": username}})
        socketio.emit("message_deleted", {
            "chat_id": str(msg["chat_id"]), "message_id": message_id, "mode": "me", "for_user": username,
        }, room=str(msg["chat_id"]))

    return jsonify({"success": True})


@app.route("/api/message/<message_id>/star", methods=["POST"])
@login_required
def api_star_message(message_id):
    username = session["username"]
    oid = to_oid(message_id)
    msg = messages_col.find_one({"_id": oid}) if oid else None
    if not msg:
        return jsonify({"error": "Message not found."}), 404

    if username in msg.get("starred_by", []):
        messages_col.update_one({"_id": oid}, {"$pull": {"starred_by": username}})
        starred = False
    else:
        messages_col.update_one({"_id": oid}, {"$addToSet": {"starred_by": username}})
        starred = True
    return jsonify({"success": True, "starred": starred})


@app.route("/api/messages/starred", methods=["GET"])
@login_required
def api_starred_messages():
    username = session["username"]
    msgs = list(messages_col.find({"starred_by": username}).sort("timestamp", DESCENDING))
    return jsonify({"messages": [serialize_message(m, username) for m in msgs]})


# --------------------------------------------------------------------------
# User / profile / search routes
# --------------------------------------------------------------------------
@app.route("/api/users", methods=["GET"])
@login_required
def api_list_users():
    username = session["username"]
    query = request.args.get("q", "").strip()
    mongo_filter = {"username": {"$ne": username}}
    if query:
        mongo_filter["username"] = {"$regex": query, "$options": "i", "$ne": username}
    users = list(users_col.find(mongo_filter).limit(30))
    return jsonify({"users": [serialize_user(u) for u in users]})


@app.route("/api/profile", methods=["GET"])
@login_required
def api_get_profile():
    user = users_col.find_one({"username": session["username"]})
    return jsonify({"user": serialize_user(user, private=True)})


@app.route("/api/profile", methods=["POST"])
@login_required
def api_update_profile():
    username = session["username"]
    data = request.get_json(silent=True) or {}
    updates = {}

    if "avatar" in data:
        updates["avatar"] = data["avatar"]
    if "about" in data:
        updates["about"] = (data["about"] or "").strip()[:140]

    if "new_username" in data and data["new_username"].strip() and data["new_username"].strip() != username:
        new_username = data["new_username"].strip()
        if users_col.find_one({"username": new_username}):
            return jsonify({"error": "That username is already taken."}), 409
        users_col.update_one({"username": username}, {"$set": {"username": new_username}})
        chats_col.update_many({"participants": username}, {"$set": {"participants.$": new_username}})
        messages_col.update_many({"sender": username}, {"$set": {"sender": new_username}})
        session["username"] = new_username
        username = new_username

    if "new_password" in data and data["new_password"]:
        current = data.get("current_password", "")
        user = users_col.find_one({"username": username})
        if not bcrypt.checkpw(current.encode("utf-8"), user["password_hash"]):
            return jsonify({"error": "Current password is incorrect."}), 403
        updates["password_hash"] = bcrypt.hashpw(data["new_password"].encode("utf-8"), bcrypt.gensalt())

    if updates:
        users_col.update_one({"username": username}, {"$set": updates})

    user = users_col.find_one({"username": username})
    return jsonify({"user": serialize_user(user, private=True)})


@app.route("/api/search", methods=["GET"])
@login_required
def api_search():
    """Unified search across chats, users, and message text."""
    username = session["username"]
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"chats": [], "users": [], "messages": []})

    # Search chats by name / group name
    all_chats = list(chats_col.find({"participants": username}))
    matched_chats = []
    for c in all_chats:
        label = c.get("group_name") if c.get("is_group") else next(
            (p for p in c["participants"] if p != username), ""
        )
        if query.lower() in (label or "").lower():
            matched_chats.append(serialize_chat(c, username))

    # Search users
    matched_users = list(users_col.find({
        "username": {"$regex": query, "$options": "i", "$ne": username}
    }).limit(15))

    # Search message text within the user's chats
    chat_ids = [c["_id"] for c in all_chats]
    matched_messages = list(messages_col.find({
        "chat_id": {"$in": chat_ids},
        "$or": [
            {"text": {"$regex": query, "$options": "i"}},
        ],
        "deleted_for_everyone": {"$ne": True},
        "deleted_for": {"$ne": username},
    }).sort("timestamp", DESCENDING).limit(30))

    return jsonify({
        "chats": matched_chats,
        "users": [serialize_user(u) for u in matched_users],
        "messages": [serialize_message(m, username) for m in matched_messages],
    })


# --------------------------------------------------------------------------
# SocketIO events (including audio calling)
# --------------------------------------------------------------------------
@socketio.on("connect")
def handle_connect():
    username = session.get("username")
    if not username:
        return False  # reject unauthenticated sockets

    online_sockets.setdefault(username, set()).add(request.sid)
    join_room(username)  # personal room for direct notifications

    users_col.update_one({"username": username}, {"$set": {"online": True}})
    emit("online_status", {"username": username, "online": True}, broadcast=True)
    return True


@socketio.on("disconnect")
def handle_disconnect():
    username = session.get("username")
    if not username:
        return
    sockets = online_sockets.get(username)
    if sockets:
        sockets.discard(request.sid)
        if not sockets:
            del online_sockets[username]

    # Clean up any active calls
    for chat_id, call in list(active_calls.items()):
        if call.get("caller") == username or call.get("callee") == username:
            del active_calls[chat_id]
            emit("call_ended", {"chat_id": chat_id}, room=chat_id)

    if username not in online_sockets:
        last_seen = now_iso()
        users_col.update_one({"username": username}, {"$set": {"online": False, "last_seen": last_seen}})
        emit("online_status", {"username": username, "online": False, "last_seen": last_seen}, broadcast=True)


@socketio.on("join")
def handle_join(data):
    """Client joins a chat room so it receives real-time events for it."""
    chat_id = data.get("chat_id")
    if chat_id:
        join_room(chat_id)


@socketio.on("leave")
def handle_leave(data):
    chat_id = data.get("chat_id")
    if chat_id:
        leave_room(chat_id)


@socketio.on("typing")
def handle_typing(data):
    username = session.get("username")
    chat_id = data.get("chat_id")
    if username and chat_id:
        emit("typing", {"chat_id": chat_id, "username": username}, room=chat_id, include_self=False)


@socketio.on("stop_typing")
def handle_stop_typing(data):
    username = session.get("username")
    chat_id = data.get("chat_id")
    if username and chat_id:
        emit("stop_typing", {"chat_id": chat_id, "username": username}, room=chat_id, include_self=False)


@socketio.on("send_message")
def handle_send_message(data):
    """Primary real-time send path. Persists the message then broadcasts it
    instantly to everyone in the chat room plus each participant's personal
    room (so their sidebar / unread badge updates even if the chat isn't open)."""
    username = session.get("username")
    if not username:
        return

    chat_id = data.get("chat_id")
    text = (data.get("text") or "").strip()
    is_voice = data.get("is_voice", False)
    voice_data = data.get("voice_data")
    voice_duration = data.get("voice_duration", 0)
    
    if not chat_id or (not text and not is_voice):
        return

    oid = to_oid(chat_id)
    chat = chats_col.find_one({"_id": oid}) if oid else None
    if not chat or not user_can_access_chat(username, chat):
        return

    msg = _persist_message(oid, username, text, data.get("reply_to"), data.get("forwarded", False), is_voice, voice_data, voice_duration)

    for participant in chat["participants"]:
        payload = serialize_message(msg, participant)
        payload["chat_preview"] = serialize_chat(chats_col.find_one({"_id": oid}), participant)
        emit("receive_message", payload, room=participant)


@socketio.on("read_receipt")
def handle_read_receipt(data):
    """Client tells the server it has read messages in a chat; we mark them
    read and notify the sender(s) so their ticks turn blue."""
    username = session.get("username")
    chat_id = data.get("chat_id")
    if not username or not chat_id:
        return

    oid = to_oid(chat_id)
    messages_col.update_many(
        {"chat_id": oid, "sender": {"$ne": username}, "read_by": {"$ne": username}},
        {"$addToSet": {"read_by": username, "delivered_to": username}},
    )
    emit("read_receipt", {"chat_id": chat_id, "reader": username}, room=chat_id, include_self=False)


# --------------------------------------------------------------------------
# Audio Call Signaling Events
# --------------------------------------------------------------------------
@socketio.on("call_user")
def handle_call_user(data):
    """Initiate a call to another user."""
    caller = session.get("username")
    if not caller:
        return
    
    chat_id = data.get("chat_id")
    callee = data.get("callee")
    
    if not chat_id or not callee:
        return
    
    # Check if callee is online
    if callee not in online_sockets:
        emit("call_failed", {"reason": "User is offline"}, room=caller)
        return
    
    # Check if there's already an active call
    if chat_id in active_calls:
        emit("call_failed", {"reason": "A call is already in progress"}, room=caller)
        return
    
    # Store call info
    active_calls[chat_id] = {
        "caller": caller,
        "callee": callee,
        "status": "ringing",
        "started_at": now_iso()
    }
    
    # Notify callee about incoming call
    emit("incoming_call", {
        "chat_id": chat_id,
        "caller": caller,
        "callee": callee
    }, room=callee)
    
    # Notify caller that the call is being placed
    emit("call_ringing", {"chat_id": chat_id}, room=caller)


@socketio.on("accept_call")
def handle_accept_call(data):
    """Accept an incoming call."""
    username = session.get("username")
    if not username:
        return
    
    chat_id = data.get("chat_id")
    if not chat_id or chat_id not in active_calls:
        return
    
    call = active_calls[chat_id]
    if call["callee"] != username:
        return
    
    call["status"] = "connected"
    
    # Notify both parties that the call is connected
    emit("call_connected", {
        "chat_id": chat_id,
        "caller": call["caller"],
        "callee": call["callee"]
    }, room=chat_id)


@socketio.on("reject_call")
def handle_reject_call(data):
    """Reject an incoming call."""
    username = session.get("username")
    if not username:
        return
    
    chat_id = data.get("chat_id")
    if not chat_id or chat_id not in active_calls:
        return
    
    call = active_calls[chat_id]
    if call["callee"] != username:
        return
    
    # Remove the call
    del active_calls[chat_id]
    
    # Notify caller that the call was rejected
    emit("call_rejected", {"chat_id": chat_id}, room=call["caller"])


@socketio.on("end_call")
def handle_end_call(data):
    """End an active call."""
    username = session.get("username")
    if not username:
        return
    
    chat_id = data.get("chat_id")
    if not chat_id or chat_id not in active_calls:
        return
    
    call = active_calls[chat_id]
    
    # Remove the call
    del active_calls[chat_id]
    
    # Notify both parties that the call ended
    emit("call_ended", {"chat_id": chat_id}, room=chat_id)


@socketio.on("webrtc_offer")
def handle_webrtc_offer(data):
    """Forward WebRTC offer from caller to callee."""
    username = session.get("username")
    if not username:
        return
    
    chat_id = data.get("chat_id")
    if not chat_id or chat_id not in active_calls:
        return
    
    call = active_calls[chat_id]
    if call["caller"] != username:
        return
    
    # Forward offer to callee
    emit("webrtc_offer", {
        "chat_id": chat_id,
        "offer": data.get("offer"),
        "from": username
    }, room=call["callee"])


@socketio.on("webrtc_answer")
def handle_webrtc_answer(data):
    """Forward WebRTC answer from callee to caller."""
    username = session.get("username")
    if not username:
        return
    
    chat_id = data.get("chat_id")
    if not chat_id or chat_id not in active_calls:
        return
    
    call = active_calls[chat_id]
    if call["callee"] != username:
        return
    
    # Forward answer to caller
    emit("webrtc_answer", {
        "chat_id": chat_id,
        "answer": data.get("answer"),
        "from": username
    }, room=call["caller"])


@socketio.on("webrtc_ice_candidate")
def handle_webrtc_ice_candidate(data):
    """Forward ICE candidates between peers."""
    username = session.get("username")
    if not username:
        return
    
    chat_id = data.get("chat_id")
    if not chat_id or chat_id not in active_calls:
        return
    
    call = active_calls[chat_id]
    target = call["caller"] if username == call["callee"] else call["callee"]
    
    # Forward ICE candidate to the other party
    emit("webrtc_ice_candidate", {
        "chat_id": chat_id,
        "candidate": data.get("candidate"),
        "from": username
    }, room=target)


# --------------------------------------------------------------------------
# Embedded HTML/CSS/JS Templates
# --------------------------------------------------------------------------
AUTH_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NovaChat</title>
<style>
  * { margin:0; padding:0; box-sizing:border-box; font-family:'Segoe UI', Roboto, Helvetica, Arial, sans-serif; }
  body {
    min-height:100vh; display:flex; align-items:center; justify-content:center;
    background: linear-gradient(135deg, #0f2027, #203a43, #2c5364);
    background-size: 400% 400%; animation: gradientShift 15s ease infinite;
  }
  @keyframes gradientShift {
    0% { background-position:0% 50%; } 50% { background-position:100% 50%; } 100% { background-position:0% 50%; }
  }
  .card {
    width:380px; padding:40px 32px; border-radius:20px;
    background: rgba(255,255,255,0.08); backdrop-filter: blur(20px);
    border:1px solid rgba(255,255,255,0.15); box-shadow: 0 8px 32px rgba(0,0,0,0.4);
    animation: fadeUp .5s ease;
  }
  @keyframes fadeUp { from { opacity:0; transform:translateY(20px);} to {opacity:1; transform:translateY(0);} }
  .logo { text-align:center; margin-bottom:8px; }
  .logo-circle {
    width:64px; height:64px; margin:0 auto 12px; border-radius:50%;
    background: linear-gradient(135deg, #25d366, #128c7e);
    display:flex; align-items:center; justify-content:center;
    font-size:28px; font-weight:700; color:white; box-shadow:0 4px 20px rgba(37,211,102,0.5);
  }
  h1 { text-align:center; color:#fff; font-size:22px; margin-bottom:4px; }
  p.sub { text-align:center; color:rgba(255,255,255,0.6); font-size:13px; margin-bottom:24px; }
  .field { margin-bottom:16px; }
  .field label { display:block; color:rgba(255,255,255,0.75); font-size:12px; margin-bottom:6px; }
  .field input {
    width:100%; padding:12px 14px; border-radius:10px; border:1px solid rgba(255,255,255,0.2);
    background: rgba(255,255,255,0.07); color:#fff; font-size:14px; outline:none; transition: all .2s;
  }
  .field input:focus { border-color:#25d366; background:rgba(255,255,255,0.12); }
  .btn {
    width:100%; padding:13px; border:none; border-radius:10px; margin-top:8px;
    background: linear-gradient(135deg, #25d366, #128c7e); color:#fff; font-size:15px; font-weight:600;
    cursor:pointer; transition: transform .15s, box-shadow .15s;
  }
  .btn:hover { transform:translateY(-1px); box-shadow:0 6px 20px rgba(37,211,102,0.4); }
  .switch-link { text-align:center; margin-top:18px; color:rgba(255,255,255,0.6); font-size:13px; }
  .switch-link a { color:#25d366; text-decoration:none; font-weight:600; }
  .error-box {
    background: rgba(255,80,80,0.15); border:1px solid rgba(255,80,80,0.4); color:#ff8a8a;
    padding:10px 12px; border-radius:8px; font-size:13px; margin-bottom:16px; display:none;
  }
</style>
</head>
<body>
  <div class="card">
    <div class="logo"><div class="logo-circle">NC</div></div>
    <h1>{{ 'Welcome back' if mode == 'login' else 'Create your account' }}</h1>
    <p class="sub">{{ 'Sign in to continue to NovaChat' if mode == 'login' else 'Join NovaChat in seconds' }}</p>
    <div class="error-box" id="errorBox"></div>
    <form id="authForm">
      <div class="field">
        <label>Username</label>
        <input type="text" id="username" autocomplete="username" required minlength="3">
      </div>
      <div class="field">
        <label>Password</label>
        <input type="password" id="password" autocomplete="current-password" required minlength="4">
      </div>
      <button type="submit" class="btn">{{ 'Log In' if mode == 'login' else 'Sign Up' }}</button>
    </form>
    <div class="switch-link">
      {% if mode == 'login' %}
        Don't have an account? <a href="/register">Sign up</a>
      {% else %}
        Already have an account? <a href="/login">Log in</a>
      {% endif %}
    </div>
  </div>

<script>
  const form = document.getElementById('authForm');
  const errorBox = document.getElementById('errorBox');
  const endpoint = "{{ '/login' if mode == 'login' else '/register' }}";

  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    errorBox.style.display = 'none';
    const username = document.getElementById('username').value.trim();
    const password = document.getElementById('password').value;

    try {
      const res = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password })
      });
      const data = await res.json();
      if (!res.ok) {
        errorBox.textContent = data.error || 'Something went wrong.';
        errorBox.style.display = 'block';
        return;
      }
      window.location.href = data.redirect;
    } catch (err) {
      errorBox.textContent = 'Network error. Please try again.';
      errorBox.style.display = 'block';
    }
  });
</script>
</body>
</html>
"""

# The CHAT_TEMPLATE with voice features and audio calling - I'll include the full version with call functionality
CHAT_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NovaChat</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.2/socket.io.min.js"></script>
<style>
:root {
  --bg-primary:#0b141a; --bg-secondary:#111b21; --bg-tertiary:#202c33;
  --panel:#111b21; --border:#222d34; --text-primary:#e9edef; --text-secondary:#8696a0;
  --accent:#25d366; --accent-dark:#128c7e; --bubble-out:#005c4b; --bubble-in:#202c33;
  --hover:rgba(255,255,255,0.06); --danger:#f15c6d;
}
body.light {
  --bg-primary:#f0f2f5; --bg-secondary:#ffffff; --bg-tertiary:#f7f8fa;
  --panel:#ffffff; --border:#e9edef; --text-primary:#111b21; --text-secondary:#667781;
  --bubble-out:#d9fdd3; --bubble-in:#ffffff; --hover:rgba(0,0,0,0.04);
}
* { margin:0; padding:0; box-sizing:border-box; font-family:'Segoe UI', Roboto, Helvetica, Arial, sans-serif; }
html, body { height:100%; overflow:hidden; background: var(--bg-primary); color:var(--text-primary); transition: background .3s, color .3s; }

::-webkit-scrollbar { width:8px; }
::-webkit-scrollbar-track { background:transparent; }
::-webkit-scrollbar-thumb { background: rgba(134,150,160,0.4); border-radius:10px; }
::-webkit-scrollbar-thumb:hover { background: rgba(134,150,160,0.7); }

.app { display:flex; height:100vh; }

/* ---------------- SIDEBAR ---------------- */
.sidebar {
  width:400px; min-width:320px; background: var(--panel); border-right:1px solid var(--border);
  display:flex; flex-direction:column; backdrop-filter: blur(10px);
}
.sidebar-header {
  display:flex; align-items:center; justify-content:space-between; padding:12px 16px;
  background: var(--bg-tertiary); border-bottom:1px solid var(--border);
}
.avatar {
  width:40px; height:40px; border-radius:50%; display:flex; align-items:center; justify-content:center;
  background: linear-gradient(135deg, var(--accent), var(--accent-dark)); color:#fff; font-weight:600;
  font-size:16px; cursor:pointer; overflow:hidden; flex-shrink:0; user-select:none;
}
.avatar img { width:100%; height:100%; object-fit:cover; }
.header-icons { display:flex; gap:6px; }
.icon-btn {
  width:38px; height:38px; border-radius:50%; border:none; background:transparent; color:var(--text-secondary);
  font-size:18px; cursor:pointer; display:flex; align-items:center; justify-content:center; transition: background .15s;
}
.icon-btn:hover { background: var(--hover); }

.search-wrap { padding:8px 12px; }
.search-box {
  display:flex; align-items:center; gap:10px; background: var(--bg-tertiary); border-radius:10px; padding:8px 14px;
}
.search-box input { flex:1; border:none; outline:none; background:transparent; color:var(--text-primary); font-size:14px; }
.search-box span { color:var(--text-secondary); }

.chat-list { flex:1; overflow-y:auto; }
.chat-item {
  display:flex; align-items:center; gap:12px; padding:12px 16px; cursor:pointer; position:relative;
  border-bottom:1px solid var(--border); transition: background .15s; animation: fadeIn .3s ease;
}
@keyframes fadeIn { from {opacity:0;} to {opacity:1;} }
.chat-item:hover { background: var(--hover); }
.chat-item.active { background: var(--bg-tertiary); }
.chat-item .meta { flex:1; min-width:0; }
.chat-item .row1 { display:flex; justify-content:space-between; align-items:center; }
.chat-item .name { font-size:15px; font-weight:500; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:220px;}
.chat-item .time { font-size:11px; color:var(--text-secondary); }
.chat-item .row2 { display:flex; justify-content:space-between; align-items:center; margin-top:2px; }
.chat-item .preview { font-size:13px; color:var(--text-secondary); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:230px; }
.badge {
  background: var(--accent); color:#062f24; font-size:11px; font-weight:700; border-radius:999px;
  padding:2px 7px; min-width:20px; text-align:center;
}
.pin-icon { font-size:12px; color:var(--text-secondary); margin-right:4px; }
.online-dot {
  width:10px; height:10px; border-radius:50%; background:var(--accent); position:absolute; left:44px; bottom:10px;
  border:2px solid var(--panel);
}

/* ---------------- MAIN CHAT AREA ---------------- */
.main {
  flex:1; display:flex; flex-direction:column; background: var(--bg-primary);
  background-image: radial-gradient(circle at 20% 20%, rgba(37,211,102,0.03), transparent 40%);
}
.empty-state { flex:1; display:flex; flex-direction:column; align-items:center; justify-content:center; color:var(--text-secondary); }
.empty-state .logo-circle { width:90px; height:90px; border-radius:50%; background: linear-gradient(135deg, var(--accent), var(--accent-dark)); display:flex; align-items:center; justify-content:center; font-size:40px; color:#fff; margin-bottom:20px; }

.chat-header {
  display:flex; align-items:center; gap:12px; padding:10px 16px; background: var(--bg-tertiary);
  border-bottom:1px solid var(--border);
}
.chat-header .info { flex:1; min-width:0; }
.chat-header .name { font-size:15px; font-weight:600; }
.chat-header .status { font-size:12.5px; color:var(--text-secondary); }

.messages {
  flex:1; overflow-y:auto; padding:20px 8%; display:flex; flex-direction:column; gap:2px;
}
.msg-row { display:flex; margin-bottom:6px; animation: bubbleIn .2s ease; }
@keyframes bubbleIn { from {opacity:0; transform:translateY(6px);} to {opacity:1; transform:translateY(0);} }
.msg-row.out { justify-content:flex-end; }
.bubble {
  max-width:65%; padding:8px 10px 6px 12px; border-radius:12px; position:relative; font-size:14.2px; line-height:1.4;
  box-shadow:0 1px 1px rgba(0,0,0,0.2); cursor:pointer;
}
.msg-row.in .bubble { background: var(--bubble-in); border-top-left-radius:2px; }
.msg-row.out .bubble { background: var(--bubble-out); border-top-right-radius:2px; }
.bubble .sender-label { font-size:12.5px; font-weight:700; color:var(--accent); margin-bottom:2px; }
.bubble .reply-box { background: rgba(0,0,0,0.15); border-left:3px solid var(--accent); padding:4px 8px; border-radius:6px; margin-bottom:5px; font-size:12.5px; opacity:0.85; }
.bubble .text { white-space:pre-wrap; word-break:break-word; }
.bubble .meta-row { display:flex; justify-content:flex-end; align-items:center; gap:4px; margin-top:2px; }
.bubble .time { font-size:10.5px; color:var(--text-secondary); }
.bubble .ticks { font-size:13px; color:var(--text-secondary); }
.bubble .ticks.read { color:#53bdeb; }
.bubble .edited-tag { font-size:10px; color:var(--text-secondary); font-style:italic; }
.bubble .star-tag { font-size:11px; }
.bubble.deleted .text { font-style:italic; color:var(--text-secondary); }

/* Voice message styles */
.voice-message {
  display:flex; align-items:center; gap:12px; padding:4px 0; min-width:160px;
}
.voice-play-btn {
  width:36px; height:36px; border-radius:50%; border:none; background: var(--accent);
  color:#fff; cursor:pointer; display:flex; align-items:center; justify-content:center;
  font-size:16px; transition: background .2s; flex-shrink:0;
}
.voice-play-btn:hover { background: var(--accent-dark); }
.voice-play-btn.playing { background: var(--danger); }
.voice-waveform {
  flex:1; height:24px; display:flex; align-items:center; gap:2px;
}
.voice-waveform .bar {
  flex:1; height:4px; border-radius:2px; background: var(--text-secondary); transition: height .15s;
}
.voice-waveform .bar.active { background: var(--accent); }
.voice-timer {
  font-size:12px; color:var(--text-secondary); min-width:40px; text-align:right;
}
.voice-duration {
  font-size:11px; color:var(--text-secondary); margin-left:4px;
}

.date-sep { text-align:center; margin:14px 0; }
.date-sep span { background: var(--bg-tertiary); color:var(--text-secondary); font-size:12px; padding:5px 12px; border-radius:8px; }

.typing-indicator { padding:6px 16px; font-size:13px; color:var(--accent); font-style:italic; min-height:22px; }

.composer {
  display:flex; align-items:flex-end; gap:8px; padding:10px 16px; background: var(--bg-tertiary); border-top:1px solid var(--border);
}
.composer textarea {
  flex:1; resize:none; max-height:120px; border:none; outline:none; background: var(--bg-secondary); color:var(--text-primary);
  border-radius:20px; padding:11px 16px; font-size:14.5px; line-height:1.4;
}
.composer .voice-controls {
  display:flex; align-items:center; gap:8px;
}
.voice-record-btn {
  width:40px; height:40px; border-radius:50%; border:none; background: var(--bg-secondary);
  color:var(--text-secondary); cursor:pointer; display:flex; align-items:center; justify-content:center;
  font-size:18px; transition: all .2s;
}
.voice-record-btn.recording {
  background: var(--danger); color:#fff; animation: pulse 1s infinite;
}
@keyframes pulse {
  0% { transform:scale(1); } 50% { transform:scale(1.05); } 100% { transform:scale(1); }
}
.voice-record-timer {
  font-size:14px; color:var(--text-secondary); min-width:60px; font-weight:600;
}
.voice-cancel-btn {
  width:32px; height:32px; border-radius:50%; border:none; background: transparent;
  color:var(--text-secondary); cursor:pointer; font-size:16px;
}
.voice-send-btn {
  width:36px; height:36px; border-radius:50%; border:none; background: var(--accent);
  color:#fff; cursor:pointer; font-size:16px; display:flex; align-items:center; justify-content:center;
}
.voice-send-btn:hover { background: var(--accent-dark); }

.reply-preview-bar {
  display:none; align-items:center; justify-content:space-between; background: var(--bg-secondary);
  padding:8px 14px; border-radius:8px; margin:0 16px 6px 16px; border-left:3px solid var(--accent);
}
.reply-preview-bar .close-reply { cursor:pointer; color:var(--text-secondary); font-size:18px; }

/* context menu */
.context-menu {
  position:fixed; background: var(--bg-tertiary); border:1px solid var(--border); border-radius:8px;
  box-shadow:0 8px 24px rgba(0,0,0,0.4); z-index:1000; overflow:hidden; min-width:170px; display:none; animation: fadeIn .12s ease;
}
.context-menu .ctx-item { padding:10px 16px; font-size:14px; cursor:pointer; display:flex; align-items:center; gap:10px; }
.context-menu .ctx-item:hover { background: var(--hover); }
.context-menu .ctx-item.danger { color: var(--danger); }

/* modals */
.modal-overlay {
  position:fixed; inset:0; background: rgba(0,0,0,0.55); display:none; align-items:center; justify-content:center; z-index:2000;
}
.modal-overlay.show { display:flex; }
.modal {
  width:420px; max-height:80vh; overflow-y:auto; background: var(--panel); border-radius:14px; padding:22px;
  box-shadow:0 10px 40px rgba(0,0,0,0.5); animation: fadeUp .25s ease; border:1px solid var(--border);
}
@keyframes fadeUp { from {opacity:0; transform:translateY(16px);} to {opacity:1; transform:translateY(0);} }
.modal h2 { margin-bottom:16px; font-size:18px; }
.modal .field { margin-bottom:14px; }
.modal label { display:block; font-size:12px; color:var(--text-secondary); margin-bottom:5px; }
.modal input, .modal textarea {
  width:100%; padding:10px 12px; border-radius:8px; border:1px solid var(--border); background: var(--bg-tertiary);
  color:var(--text-primary); font-size:14px; outline:none;
}
.modal .btn {
  padding:10px 18px; border:none; border-radius:8px; background: linear-gradient(135deg, var(--accent), var(--accent-dark));
  color:#fff; font-weight:600; cursor:pointer; font-size:14px;
}
.modal .btn.secondary { background: var(--bg-tertiary); color: var(--text-primary); }
.modal .btn-row { display:flex; justify-content:flex-end; gap:10px; margin-top:10px; }
.modal .close-x { float:right; cursor:pointer; color:var(--text-secondary); font-size:20px; }
.user-pick-list { max-height:220px; overflow-y:auto; margin-top:6px; }
.user-pick-item { display:flex; align-items:center; gap:10px; padding:8px; border-radius:8px; cursor:pointer; }
.user-pick-item:hover { background: var(--hover); }
.user-pick-item input { width:auto; }
.avatar-upload { display:flex; flex-direction:column; align-items:center; gap:10px; margin-bottom:14px; }
.avatar-upload .avatar { width:90px; height:90px; font-size:32px; }

.search-results-panel { padding:8px; }
.search-section-title { font-size:12px; color:var(--text-secondary); padding:8px 12px 4px; text-transform:uppercase; letter-spacing:.5px;}

/* toast */
.toast {
  position:fixed; bottom:24px; right:24px; background: var(--bg-tertiary); color:var(--text-primary); padding:12px 18px;
  border-radius:10px; box-shadow:0 6px 20px rgba(0,0,0,0.4); z-index:3000; animation: fadeUp .2s ease; border-left:4px solid var(--accent);
}

/* emoji picker */
.emoji-picker {
  position:absolute; bottom:64px; background: var(--bg-tertiary); border:1px solid var(--border); border-radius:12px;
  padding:10px; width:280px; max-height:220px; overflow-y:auto; display:none; grid-template-columns:repeat(8,1fr); gap:4px;
  box-shadow:0 8px 24px rgba(0,0,0,0.4); z-index:500;
}
.emoji-picker span { cursor:pointer; font-size:19px; text-align:center; padding:4px; border-radius:6px; }
.emoji-picker span:hover { background: var(--hover); }

.loader { border:3px solid var(--border); border-top:3px solid var(--accent); border-radius:50%; width:22px; height:22px; animation:spin 0.8s linear infinite; margin:10px auto; }
@keyframes spin { to { transform:rotate(360deg); } }

/* ---------------- CALL UI ---------------- */
.call-overlay {
  position:fixed; inset:0; background: rgba(0,0,0,0.85); display:none; align-items:center; justify-content:center; z-index:5000;
  flex-direction:column; gap:20px;
}
.call-overlay.show { display:flex; }
.call-overlay .call-avatar {
  width:120px; height:120px; border-radius:50%; display:flex; align-items:center; justify-content:center;
  background: linear-gradient(135deg, var(--accent), var(--accent-dark));
  font-size:48px; color:#fff; font-weight:700;
}
.call-overlay .call-name { font-size:24px; color:var(--text-primary); }
.call-overlay .call-status { font-size:16px; color:var(--text-secondary); }
.call-overlay .call-buttons { display:flex; gap:24px; margin-top:20px; }
.call-overlay .call-btn {
  width:60px; height:60px; border-radius:50%; border:none; cursor:pointer; font-size:24px;
  display:flex; align-items:center; justify-content:center; transition: transform .15s;
}
.call-overlay .call-btn:hover { transform:scale(1.05); }
.call-btn.end-call { background: var(--danger); color:#fff; }
.call-btn.accept-call { background: var(--accent); color:#fff; }
.call-btn.mute-call { background: var(--bg-tertiary); color:var(--text-primary); }
.call-btn.mute-call.muted { background: var(--danger); color:#fff; }

.call-timer { font-size:18px; color:var(--text-secondary); }

@media (max-width: 820px) {
  .sidebar { width:100%; position:absolute; z-index:50; height:100%; }
  .sidebar.hide-mobile { display:none; }
  .main { width:100%; }
}
</style>
</head>
<body>
<div class="app">

  <!-- ===================== SIDEBAR ===================== -->
  <div class="sidebar" id="sidebar">
    <div class="sidebar-header">
      <div class="avatar" id="myAvatar" onclick="openProfileModal()">{{ username[0]|upper }}</div>
      <div class="header-icons">
        <button class="icon-btn" title="New chat" onclick="openNewChatModal()">💬</button>
        <button class="icon-btn" title="New group" onclick="openNewGroupModal()">👥</button>
        <button class="icon-btn" title="Starred messages" onclick="openStarredModal()">⭐</button>
        <button class="icon-btn" title="Toggle theme" onclick="toggleTheme()">🌓</button>
        <button class="icon-btn" title="Settings" onclick="openProfileModal()">⚙️</button>
        <button class="icon-btn" title="Log out" onclick="window.location.href='/logout'">⏻</button>
      </div>
    </div>
    <div class="search-wrap">
      <div class="search-box">
        <span>🔍</span>
        <input type="text" id="searchInput" placeholder="Search chats, users, or messages">
      </div>
    </div>
    <div class="chat-list" id="chatList"><div class="loader"></div></div>
  </div>

  <!-- ===================== MAIN AREA ===================== -->
  <div class="main" id="mainArea">
    <div class="empty-state" id="emptyState">
      <div class="logo-circle">NC</div>
      <h2>NovaChat Web</h2>
      <p style="margin-top:8px;max-width:320px;text-align:center;">Select a chat to start messaging, or create a new conversation.</p>
    </div>

    <div id="chatView" style="display:none; flex-direction:column; height:100%;">
      <div class="chat-header">
        <div class="avatar" id="chatAvatar" onclick="openGroupInfoIfGroup()">?</div>
        <div class="info">
          <div class="name" id="chatName">-</div>
          <div class="status" id="chatStatus">-</div>
        </div>
        <div class="header-icons">
          <button class="icon-btn" title="Audio call" onclick="startAudioCall()" id="callBtn">📞</button>
          <button class="icon-btn" title="Search in chat" onclick="focusInlineSearch()">🔍</button>
          <button class="icon-btn" title="Pin chat" onclick="toggleCurrentChatFlag('pin')">📌</button>
          <button class="icon-btn" title="Mute chat" onclick="toggleCurrentChatFlag('mute')">🔇</button>
          <button class="icon-btn" title="Archive chat" onclick="toggleCurrentChatFlag('archive')">🗄️</button>
          <button class="icon-btn" title="Delete chat" onclick="deleteCurrentChat()">🗑️</button>
        </div>
      </div>

      <div class="messages" id="messagesPane">
        <div class="loader" id="msgLoader" style="display:none;"></div>
      </div>
      <div class="typing-indicator" id="typingIndicator"></div>

      <div class="reply-preview-bar" id="replyBar">
        <div>
          <div style="font-size:12px;color:var(--accent);font-weight:600;" id="replyBarSender"></div>
          <div style="font-size:13px;color:var(--text-secondary);" id="replyBarText"></div>
        </div>
        <span class="close-reply" onclick="cancelReply()">✕</span>
      </div>

      <div class="composer" style="position:relative;">
        <button class="icon-btn" onclick="toggleEmojiPicker()">😊</button>
        <textarea id="messageInput" rows="1" placeholder="Type a message"></textarea>
        <div class="voice-controls">
          <button class="voice-record-btn" id="voiceRecordBtn" onclick="toggleVoiceRecording()" title="Voice message">🎤</button>
          <span class="voice-record-timer" id="voiceTimer" style="display:none;">0:00</span>
          <button class="voice-cancel-btn" id="voiceCancelBtn" onclick="cancelVoiceRecording()" style="display:none;" title="Cancel recording">✕</button>
          <button class="voice-send-btn" id="voiceSendBtn" onclick="sendVoiceMessage()" style="display:none;" title="Send voice message">➤</button>
          <button class="icon-btn" title="Send" onclick="sendCurrentMessage()" style="background:var(--accent);color:#fff;">➤</button>
        </div>
        <div class="emoji-picker" id="emojiPicker"></div>
      </div>
    </div>
  </div>
</div>

<!-- ===================== CALL OVERLAY ===================== -->
<div class="call-overlay" id="callOverlay">
  <div class="call-avatar" id="callAvatar">?</div>
  <div class="call-name" id="callName">Calling...</div>
  <div class="call-status" id="callStatus">Connecting...</div>
  <div class="call-timer" id="callTimer" style="display:none;">00:00</div>
  <div class="call-buttons">
    <button class="call-btn mute-call" id="muteBtn" onclick="toggleMute()" title="Mute microphone">🎙️</button>
    <button class="call-btn end-call" onclick="endCall()" title="End call">📞</button>
    <button class="call-btn accept-call" id="acceptCallBtn" style="display:none;" onclick="acceptCall()" title="Accept call">📞</button>
  </div>
</div>

<!-- ===================== CONTEXT MENU ===================== -->
<div class="context-menu" id="messageContextMenu">
  <div class="ctx-item" onclick="ctxAction('reply')">↩️ Reply</div>
  <div class="ctx-item" onclick="ctxAction('forward')">➡️ Forward</div>
  <div class="ctx-item" onclick="ctxAction('copy')">📋 Copy</div>
  <div class="ctx-item" onclick="ctxAction('star')">⭐ Star</div>
  <div class="ctx-item" id="ctxEditItem" onclick="ctxAction('edit')">✏️ Edit</div>
  <div class="ctx-item danger" id="ctxDeleteMeItem" onclick="ctxAction('delete_me')">🗑️ Delete for me</div>
  <div class="ctx-item danger" id="ctxDeleteAllItem" onclick="ctxAction('delete_everyone')">🗑️ Delete for everyone</div>
</div>

<!-- ===================== PROFILE MODAL ===================== -->
<div class="modal-overlay" id="profileModal">
  <div class="modal">
    <span class="close-x" onclick="closeModal('profileModal')">✕</span>
    <h2>Your Profile</h2>
    <div class="avatar-upload">
      <div class="avatar" id="profileAvatarPreview">{{ username[0]|upper }}</div>
      <input type="file" id="avatarFileInput" accept="image/*" onchange="handleAvatarUpload(event)">
    </div>
    <div class="field"><label>Username</label><input type="text" id="profileUsername"></div>
    <div class="field"><label>About</label><input type="text" id="profileAbout" maxlength="140"></div>
    <hr style="border-color:var(--border);margin:14px 0;">
    <div class="field"><label>Current Password</label><input type="password" id="profileCurrentPassword" placeholder="Required to change password"></div>
    <div class="field"><label>New Password</label><input type="password" id="profileNewPassword" placeholder="Leave blank to keep current password"></div>
    <div class="btn-row">
      <button class="btn secondary" onclick="closeModal('profileModal')">Cancel</button>
      <button class="btn" onclick="saveProfile()">Save Changes</button>
    </div>
  </div>
</div>

<!-- ===================== NEW CHAT MODAL ===================== -->
<div class="modal-overlay" id="newChatModal">
  <div class="modal">
    <span class="close-x" onclick="closeModal('newChatModal')">✕</span>
    <h2>Start a new chat</h2>
    <div class="field"><input type="text" id="newChatSearch" placeholder="Search users..." oninput="searchUsersForNewChat()"></div>
    <div class="user-pick-list" id="newChatUserList"></div>
  </div>
</div>

<!-- ===================== NEW GROUP MODAL ===================== -->
<div class="modal-overlay" id="newGroupModal">
  <div class="modal">
    <span class="close-x" onclick="closeModal('newGroupModal')">✕</span>
    <h2>Create new group</h2>
    <div class="field"><label>Group name</label><input type="text" id="groupNameInput"></div>
    <div class="field"><label>Description</label><input type="text" id="groupDescInput"></div>
    <div class="field"><label>Add members</label><input type="text" id="groupMemberSearch" placeholder="Search users..." oninput="searchUsersForGroup()"></div>
    <div class="user-pick-list" id="groupUserList"></div>
    <div class="btn-row">
      <button class="btn secondary" onclick="closeModal('newGroupModal')">Cancel</button>
      <button class="btn" onclick="createGroup()">Create Group</button>
    </div>
  </div>
</div>

<!-- ===================== GROUP INFO MODAL ===================== -->
<div class="modal-overlay" id="groupInfoModal">
  <div class="modal">
    <span class="close-x" onclick="closeModal('groupInfoModal')">✕</span>
    <h2 id="groupInfoName">Group</h2>
    <p style="color:var(--text-secondary);font-size:13px;margin-bottom:12px;" id="groupInfoDesc"></p>
    <div class="field"><label>Add member</label><input type="text" id="groupAddInput" placeholder="username" onkeydown="if(event.key==='Enter')addGroupMember()"></div>
    <div id="groupMembersList"></div>
  </div>
</div>

<!-- ===================== STARRED MODAL ===================== -->
<div class="modal-overlay" id="starredModal">
  <div class="modal">
    <span class="close-x" onclick="closeModal('starredModal')">✕</span>
    <h2>Starred Messages</h2>
    <div id="starredList"></div>
  </div>
</div>

<!-- ===================== SEARCH RESULTS MODAL ===================== -->
<div class="modal-overlay" id="searchModal">
  <div class="modal" style="width:480px;">
    <span class="close-x" onclick="closeModal('searchModal')">✕</span>
    <h2>Search results</h2>
    <div class="search-results-panel" id="searchResultsPanel"></div>
  </div>
</div>

<script>
/* =========================================================================
   NOVACHAT CLIENT LOGIC
   ========================================================================= */
const CURRENT_USER = "{{ username }}";
const socket = io();

let chats = {};             // chat_id -> chat object
let currentChatId = null;
let currentChat = null;
let replyTarget = null;     // message being replied to
let typingTimeout = null;
let oldestLoadedTimestamp = null;
let hasMoreMessages = true;
let isLoadingMessages = false;
let contextTargetMessageId = null;
let contextTargetMessageText = "";
let contextTargetIsMine = false;

// Voice recording state
let mediaRecorder = null;
let audioChunks = [];
let isRecording = false;
let recordingStartTime = null;
let recordingTimer = null;
let recordedBlob = null;
let voiceDuration = 0;

// Audio playback state
let activeAudio = null;
let activeAudioMessageId = null;

// Call state
let localStream = null;
let remoteStream = null;
let peerConnection = null;
let isCallActive = false;
let isCaller = false;
let callTimerInterval = null;
let callSeconds = 0;
let isMuted = false;
let pendingCallData = null;

const EMOJIS = ["😀","😁","😂","🤣","😊","😍","😘","😜","🤔","😎","😢","😭","😡","👍","👎","🙏","👏","🎉","❤️","🔥","💯","✅","❌","🎂","😴","🥳","🤝","👀","💡","🚀"];

/* ---------------- INITIALIZATION ---------------- */
document.addEventListener('DOMContentLoaded', () => {
  applyStoredTheme();
  buildEmojiPicker();
  loadChats();
  bindGlobalEvents();
  requestNotificationPermission();
  initVoiceRecording();
  initCallHandlers();
});

function bindGlobalEvents() {
  const input = document.getElementById('messageInput');
  input.addEventListener('input', () => {
    autoResizeTextarea(input);
    handleTypingEvent();
  });
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendCurrentMessage();
    }
  });

  document.getElementById('searchInput').addEventListener('input', debounce(handleGlobalSearch, 350));

  document.addEventListener('click', (e) => {
    const menu = document.getElementById('messageContextMenu');
    if (!menu.contains(e.target)) menu.style.display = 'none';
    const picker = document.getElementById('emojiPicker');
    if (!picker.contains(e.target) && e.target.textContent !== '😊') picker.style.display = 'none';
  });
}

function autoResizeTextarea(el) {
  el.style.height = 'auto';
  el.style.height = Math.min(el.scrollHeight, 120) + 'px';
}

function debounce(fn, delay) {
  let t;
  return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), delay); };
}

/* ---------------- THEME ---------------- */
function applyStoredTheme() {
  const theme = localStorage.getItem('novachat_theme') || 'dark';
  document.body.classList.toggle('light', theme === 'light');
}
function toggleTheme() {
  const isLight = document.body.classList.toggle('light');
  localStorage.setItem('novachat_theme', isLight ? 'light' : 'dark');
}

/* ---------------- CHAT LIST ---------------- */
async function loadChats() {
  const res = await fetch('/api/chats');
  const data = await res.json();
  chats = {};
  data.chats.forEach(c => chats[c.id] = c);
  renderChatList();
}

function renderChatList() {
  const list = document.getElementById('chatList');
  const visibleChats = Object.values(chats).filter(c => !c.archived);
  if (visibleChats.length === 0) {
    list.innerHTML = '<div style="padding:24px;text-align:center;color:var(--text-secondary);font-size:13px;">No chats yet. Start a new conversation!</div>';
    return;
  }
  list.innerHTML = '';
  visibleChats.forEach(c => list.appendChild(buildChatItemEl(c)));
}

function buildChatItemEl(c) {
  const el = document.createElement('div');
  el.className = 'chat-item' + (c.id === currentChatId ? ' active' : '');
  el.onclick = () => openChat(c.id);

  const initials = (c.name || '?')[0].toUpperCase();
  const avatarHtml = c.avatar ? `<img src="${c.avatar}">` : initials;
  let lastMsgText = c.last_message ? escapeHtml(c.last_message.text || '') : 'No messages yet';
  const lastMsgPrefix = c.last_message && c.last_message.sender === CURRENT_USER ? 'You: ' : '';
  const time = c.last_message ? formatTime(c.last_message.timestamp) : '';

  el.innerHTML = `
    <div class="avatar" style="position:relative;">${avatarHtml}</div>
    ${!c.is_group && c.online ? '<span class="online-dot"></span>' : ''}
    <div class="meta">
      <div class="row1">
        <span class="name">${c.pinned ? '<span class="pin-icon">📌</span>' : ''}${escapeHtml(c.name)}</span>
        <span class="time">${time}</span>
      </div>
      <div class="row2">
        <span class="preview">${c.muted ? '🔇 ' : ''}${lastMsgPrefix}${lastMsgText}</span>
        ${c.unread_count > 0 ? `<span class="badge">${c.unread_count}</span>` : ''}
      </div>
    </div>
  `;

  el.oncontextmenu = (e) => { e.preventDefault(); showChatQuickMenu(e, c.id); };
  return el;
}

function showChatQuickMenu(e, chatId) {
  const choice = prompt("Type: pin / mute / archive / delete");
  if (!choice) return;
  const action = choice.trim().toLowerCase();
  if (['pin', 'mute', 'archive'].includes(action)) {
    fetch(`/api/chats/${chatId}/${action}`, { method: 'POST' }).then(() => loadChats());
  } else if (action === 'delete') {
    fetch(`/api/chats/${chatId}`, { method: 'DELETE' }).then(() => { loadChats(); if (currentChatId===chatId) showEmptyState(); });
  }
}

/* ---------------- OPENING A CHAT ---------------- */
async function openChat(chatId) {
  if (currentChatId) socket.emit('leave', { chat_id: currentChatId });
  currentChatId = chatId;
  currentChat = chats[chatId];
  oldestLoadedTimestamp = null;
  hasMoreMessages = true;

  socket.emit('join', { chat_id: chatId });

  document.getElementById('emptyState').style.display = 'none';
  document.getElementById('chatView').style.display = 'flex';

  renderChatHeader();
  renderChatList();

  const pane = document.getElementById('messagesPane');
  pane.innerHTML = '<div class="loader"></div>';
  await loadMessages(chatId, null, true);
  markChatAsRead(chatId);
  
  // Show/hide call button based on whether it's a direct chat
  const callBtn = document.getElementById('callBtn');
  callBtn.style.display = currentChat.is_group ? 'none' : 'flex';
}

function renderChatHeader() {
  const c = currentChat;
  document.getElementById('chatName').textContent = c.name;
  document.getElementById('chatAvatar').innerHTML = c.avatar ? `<img src="${c.avatar}">` : (c.name[0] || '?').toUpperCase();
  if (c.is_group) {
    document.getElementById('chatStatus').textContent = `${c.members.length} members`;
  } else {
    document.getElementById('chatStatus').textContent = c.online ? 'online' : formatLastSeen(c.last_seen);
  }
}

function showEmptyState() {
  currentChatId = null;
  currentChat = null;
  document.getElementById('chatView').style.display = 'none';
  document.getElementById('emptyState').style.display = 'flex';
}

/* ---------------- MESSAGES ---------------- */
async function loadMessages(chatId, before, replace) {
  if (isLoadingMessages) return;
  isLoadingMessages = true;
  const pane = document.getElementById('messagesPane');

  let url = `/api/messages/${chatId}?limit=30`;
  if (before) url += `&before=${encodeURIComponent(before)}`;

  const res = await fetch(url);
  const data = await res.json();
  hasMoreMessages = data.has_more;

  if (replace) pane.innerHTML = '';
  const scrollAnchor = pane.scrollHeight;

  data.messages.forEach(m => {
    if (m.timestamp && (!oldestLoadedTimestamp || m.timestamp < oldestLoadedTimestamp)) {
      oldestLoadedTimestamp = m.timestamp;
    }
    const el = buildMessageEl(m);
    if (before) pane.insertBefore(el, pane.firstChild);
    else pane.appendChild(el);
  });

  if (replace) {
    pane.scrollTop = pane.scrollHeight;
    pane.onscroll = () => {
      if (pane.scrollTop < 60 && hasMoreMessages && !isLoadingMessages) {
        loadMessages(chatId, oldestLoadedTimestamp, false);
      }
    };
  } else {
    pane.scrollTop = pane.scrollHeight - scrollAnchor;
  }
  isLoadingMessages = false;
}

function buildMessageEl(m) {
  const wrap = document.createElement('div');
  const isOut = m.sender === CURRENT_USER;
  wrap.className = 'msg-row ' + (isOut ? 'out' : 'in');
  wrap.dataset.id = m.id;

  const bubble = document.createElement('div');
  bubble.className = 'bubble' + ((m.deleted_for_everyone || m.hidden_for_me) ? ' deleted' : '');

  let inner = '';
  if (currentChat && currentChat.is_group && !isOut) {
    inner += `<div class="sender-label">${escapeHtml(m.sender)}</div>`;
  }
  if (m.reply_preview) {
    inner += `<div class="reply-box"><b>${escapeHtml(m.reply_preview.sender)}</b><br>${escapeHtml(m.reply_preview.text)}</div>`;
  }
  if (m.forwarded) {
    inner += `<div style="font-size:11px;color:var(--text-secondary);font-style:italic;">➡️ Forwarded</div>`;
  }

  // Handle voice messages
  if (m.is_voice && m.voice_data && !m.deleted_for_everyone && !m.hidden_for_me) {
    inner += buildVoiceMessageHTML(m);
  } else {
    let displayText;
    if (m.deleted_for_everyone) displayText = 'This message was deleted';
    else if (m.hidden_for_me) displayText = 'You deleted this message';
    else displayText = escapeHtml(m.text);
    inner += `<div class="text">${linkify(displayText)}</div>`;
  }

  const isStarred = m.starred_by && m.starred_by.includes(CURRENT_USER);
  const ticks = isOut ? tickHtml(m) : '';
  inner += `<div class="meta-row">
      ${isStarred ? '<span class="star-tag">⭐</span>' : ''}
      ${m.edited ? '<span class="edited-tag">edited</span>' : ''}
      <span class="time">${formatTime(m.timestamp)}</span>
      ${ticks}
    </div>`;

  bubble.innerHTML = inner;
  bubble.oncontextmenu = (e) => { e.preventDefault(); openMessageContextMenu(e, m); };
  bubble.ondblclick = () => openMessageContextMenu({clientX: 0, clientY: 0, target: bubble}, m, true);
  wrap.appendChild(bubble);
  return wrap;
}

function buildVoiceMessageHTML(m) {
  const duration = m.voice_duration || 0;
  const minutes = Math.floor(duration / 60);
  const seconds = Math.floor(duration % 60);
  const durationStr = `${minutes}:${seconds.toString().padStart(2, '0')}`;
  
  let bars = '';
  for (let i = 0; i < 16; i++) {
    const height = 4 + Math.random() * 16;
    bars += `<div class="bar" data-index="${i}" style="height:${height}px;"></div>`;
  }
  
  return `
    <div class="voice-message" data-message-id="${m.id}">
      <button class="voice-play-btn" onclick="toggleVoicePlayback('${m.id}', '${m.voice_data}')" data-playing="false">▶</button>
      <div class="voice-waveform">${bars}</div>
      <span class="voice-timer">${durationStr}</span>
    </div>
  `;
}

function tickHtml(m) {
  if (m.read_by && m.read_by.filter(u => u !== CURRENT_USER).length > 0) {
    return '<span class="ticks read">✓✓</span>';
  } else if (m.delivered_to && m.delivered_to.filter(u => u !== CURRENT_USER).length > 0) {
    return '<span class="ticks">✓✓</span>';
  }
  return '<span class="ticks">✓</span>';
}

function linkify(text) {
  return text.replace(/(https?:\\/\\/[^\\s]+)/g, '<a href="$1" target="_blank" style="color:#53bdeb;">$1</a>');
}

/* ---------------- VOICE MESSAGE PLAYBACK ---------------- */
function toggleVoicePlayback(messageId, voiceData) {
  const btn = document.querySelector(`.voice-message[data-message-id="${messageId}"] .voice-play-btn`);
  const waveform = document.querySelector(`.voice-message[data-message-id="${messageId}"] .voice-waveform`);
  
  if (activeAudio && activeAudioMessageId !== messageId) {
    activeAudio.pause();
    const oldBtn = document.querySelector(`.voice-message[data-message-id="${activeAudioMessageId}"] .voice-play-btn`);
    if (oldBtn) {
      oldBtn.textContent = '▶';
      oldBtn.classList.remove('playing');
    }
  }
  
  if (activeAudio && activeAudioMessageId === messageId && !activeAudio.paused) {
    activeAudio.pause();
    btn.textContent = '▶';
    btn.classList.remove('playing');
    return;
  }
  
  try {
    const audioData = voiceData.split(',')[1] || voiceData;
    const binaryString = atob(audioData);
    const bytes = new Uint8Array(binaryString.length);
    for (let i = 0; i < binaryString.length; i++) {
      bytes[i] = binaryString.charCodeAt(i);
    }
    const audioBlob = new Blob([bytes], { type: 'audio/webm' });
    const audioUrl = URL.createObjectURL(audioBlob);
    
    if (activeAudio) {
      activeAudio.pause();
      activeAudio = null;
    }
    
    activeAudio = new Audio(audioUrl);
    activeAudioMessageId = messageId;
    
    activeAudio.onplay = () => {
      btn.textContent = '⏸';
      btn.classList.add('playing');
      animateWaveform(waveform, true);
    };
    
    activeAudio.onpause = () => {
      btn.textContent = '▶';
      btn.classList.remove('playing');
      animateWaveform(waveform, false);
    };
    
    activeAudio.onended = () => {
      btn.textContent = '▶';
      btn.classList.remove('playing');
      animateWaveform(waveform, false);
      activeAudio = null;
      activeAudioMessageId = null;
      URL.revokeObjectURL(audioUrl);
    };
    
    activeAudio.play();
  } catch (e) {
    console.error('Error playing voice message:', e);
    showToast('Could not play voice message');
  }
}

function animateWaveform(waveform, active) {
  if (!waveform) return;
  const bars = waveform.querySelectorAll('.bar');
  if (active) {
    let index = 0;
    const interval = setInterval(() => {
      if (!activeAudio || activeAudio.paused) {
        clearInterval(interval);
        bars.forEach(bar => bar.classList.remove('active'));
        return;
      }
      bars.forEach((bar, i) => {
        bar.classList.toggle('active', i === index % bars.length);
      });
      index++;
    }, 100);
    waveform.dataset.interval = interval;
  } else {
    const interval = waveform.dataset.interval;
    if (interval) clearInterval(interval);
    bars.forEach(bar => bar.classList.remove('active'));
  }
}

/* ---------------- VOICE RECORDING ---------------- */
function initVoiceRecording() {
  if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
    document.getElementById('voiceRecordBtn').style.display = 'none';
    return;
  }
}

async function toggleVoiceRecording() {
  if (isRecording) {
    stopVoiceRecording();
  } else {
    startVoiceRecording();
  }
}

async function startVoiceRecording() {
  try {
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    
    mediaRecorder = new MediaRecorder(stream);
    audioChunks = [];
    
    mediaRecorder.ondataavailable = (event) => {
      if (event.data.size > 0) {
        audioChunks.push(event.data);
      }
    };
    
    mediaRecorder.onstop = () => {
      const audioBlob = new Blob(audioChunks, { type: 'audio/webm' });
      recordedBlob = audioBlob;
      voiceDuration = (Date.now() - recordingStartTime) / 1000;
      
      document.getElementById('voiceSendBtn').style.display = 'flex';
      document.getElementById('voiceCancelBtn').style.display = 'flex';
      
      stream.getTracks().forEach(track => track.stop());
    };
    
    mediaRecorder.start();
    isRecording = true;
    recordingStartTime = Date.now();
    
    const btn = document.getElementById('voiceRecordBtn');
    btn.classList.add('recording');
    btn.textContent = '⏹';
    
    const timer = document.getElementById('voiceTimer');
    timer.style.display = 'inline';
    startRecordingTimer();
    
    document.getElementById('messageInput').style.display = 'none';
    document.querySelector('.composer .icon-btn:last-child').style.display = 'none';
    
    showToast('Recording voice message...');
  } catch (err) {
    console.error('Error accessing microphone:', err);
    showToast('Could not access microphone. Please check permissions.');
  }
}

function stopVoiceRecording() {
  if (mediaRecorder && isRecording) {
    mediaRecorder.stop();
    isRecording = false;
    stopRecordingTimer();
    
    const btn = document.getElementById('voiceRecordBtn');
    btn.classList.remove('recording');
    btn.textContent = '🎤';
  }
}

function cancelVoiceRecording() {
  if (mediaRecorder) {
    mediaRecorder.stop();
    isRecording = false;
    stopRecordingTimer();
  }
  
  audioChunks = [];
  recordedBlob = null;
  voiceDuration = 0;
  
  document.getElementById('voiceRecordBtn').classList.remove('recording');
  document.getElementById('voiceRecordBtn').textContent = '🎤';
  document.getElementById('voiceTimer').style.display = 'none';
  document.getElementById('voiceSendBtn').style.display = 'none';
  document.getElementById('voiceCancelBtn').style.display = 'none';
  document.getElementById('messageInput').style.display = 'block';
  document.querySelector('.composer .icon-btn:last-child').style.display = 'flex';
  
  showToast('Recording cancelled');
}

function startRecordingTimer() {
  let seconds = 0;
  const timer = document.getElementById('voiceTimer');
  timer.textContent = '0:00';
  
  recordingTimer = setInterval(() => {
    seconds++;
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    timer.textContent = `${mins}:${secs.toString().padStart(2, '0')}`;
    
    if (seconds >= 60) {
      stopVoiceRecording();
      showToast('Maximum recording time reached (60 seconds)');
    }
  }, 1000);
}

function stopRecordingTimer() {
  if (recordingTimer) {
    clearInterval(recordingTimer);
    recordingTimer = null;
  }
}

async function sendVoiceMessage() {
  if (!recordedBlob || !currentChatId) return;
  
  try {
    const reader = new FileReader();
    reader.onload = () => {
      const base64Data = reader.result;
      
      socket.emit('send_message', {
        chat_id: currentChatId,
        text: '',
        is_voice: true,
        voice_data: base64Data,
        voice_duration: Math.round(voiceDuration)
      });
      
      document.getElementById('voiceSendBtn').style.display = 'none';
      document.getElementById('voiceCancelBtn').style.display = 'none';
      document.getElementById('voiceTimer').style.display = 'none';
      document.getElementById('messageInput').style.display = 'block';
      document.querySelector('.composer .icon-btn:last-child').style.display = 'flex';
      
      recordedBlob = null;
      voiceDuration = 0;
      
      showToast('Voice message sent');
    };
    reader.readAsDataURL(recordedBlob);
  } catch (e) {
    console.error('Error sending voice message:', e);
    showToast('Failed to send voice message');
  }
}

/* ---------------- SENDING MESSAGES ---------------- */
let isSending = false;

function sendCurrentMessage() {
    if (isSending) {
        console.log("Blocked duplicate send attempt");
        return;
    }
    isSending = true;

    const input = document.getElementById('messageInput');
    const text = input.value.trim();
    if (!text || !currentChatId) {
        isSending = false;
        return;
    }

    socket.emit('send_message', {
        chat_id: currentChatId,
        text: text,
        reply_to: replyTarget ? replyTarget.id : null,
    });

    setTimeout(() => {
        isSending = false;
        console.log("Send lock released");
    }, 500);

    input.value = '';
    autoResizeTextarea(input);
    cancelReply();
    socket.emit('stop_typing', { chat_id: currentChatId });
}

/* ---------------- SOCKET EVENT HANDLERS ---------------- */
socket.on('receive_message', (m) => {
  if (m.chat_preview) {
    chats[m.chat_preview.id] = m.chat_preview;
  } else if (chats[m.chat_id]) {
    const previewText = m.text || (m.is_voice ? '🎤 Voice message' : '');
    chats[m.chat_id].last_message = { text: previewText, sender: m.sender, timestamp: m.timestamp };
    chats[m.chat_id].updated_at = m.timestamp;
  }
  renderChatList();

  if (m.chat_id === currentChatId) {
    const pane = document.getElementById('messagesPane');
    const nearBottom = pane.scrollHeight - pane.scrollTop - pane.clientHeight < 150;
    pane.appendChild(buildMessageEl(m));
    if (nearBottom || m.sender === CURRENT_USER) pane.scrollTop = pane.scrollHeight;
    if (m.sender !== CURRENT_USER) markChatAsRead(m.chat_id);
  }

  if (m.sender !== CURRENT_USER) {
    playNotificationSound();
    showDesktopNotification(m);
  }
});

socket.on('message_edited', (data) => {
  const row = document.querySelector(`.msg-row[data-id="${data.message_id}"] .text`);
  if (row) row.innerHTML = linkify(escapeHtml(data.text));
});

socket.on('message_deleted', (data) => {
  const row = document.querySelector(`.msg-row[data-id="${data.message_id}"] .bubble`);
  if (!row) return;
  if (data.mode === 'everyone' || data.for_user === CURRENT_USER) {
    row.classList.add('deleted');
    row.querySelector('.text').textContent = data.mode === 'everyone' ? 'This message was deleted' : 'You deleted this message';
  }
});

socket.on('typing', (data) => {
  if (data.chat_id === currentChatId) {
    document.getElementById('typingIndicator').textContent = `${data.username} is typing...`;
  }
});
socket.on('stop_typing', (data) => {
  if (data.chat_id === currentChatId) {
    document.getElementById('typingIndicator').textContent = '';
  }
});

socket.on('online_status', (data) => {
  Object.values(chats).forEach(c => {
    if (!c.is_group && c.name === data.username) {
      c.online = data.online;
      c.last_seen = data.last_seen;
    }
  });
  if (currentChat && !currentChat.is_group && currentChat.name === data.username) {
    currentChat.online = data.online;
    currentChat.last_seen = data.last_seen;
    renderChatHeader();
  }
  renderChatList();
});

socket.on('read_receipt', (data) => {
  if (data.chat_id === currentChatId) {
    document.querySelectorAll('.msg-row.out .ticks').forEach(el => {
      el.classList.add('read');
      el.textContent = '✓✓';
    });
  }
});

socket.on('group_updated', (data) => { if (data.chat_id === currentChatId) loadChats(); });

/* ---------------- TYPING ---------------- */
function handleTypingEvent() {
  if (!currentChatId) return;
  socket.emit('typing', { chat_id: currentChatId });
  clearTimeout(typingTimeout);
  typingTimeout = setTimeout(() => socket.emit('stop_typing', { chat_id: currentChatId }), 1500);
}

/* ---------------- READ RECEIPTS ---------------- */
function markChatAsRead(chatId) {
  socket.emit('read_receipt', { chat_id: chatId });
  if (chats[chatId]) chats[chatId].unread_count = 0;
  renderChatList();
}

/* ---------------- MESSAGE CONTEXT MENU ---------------- */
function openMessageContextMenu(e, m, forceShow) {
  contextTargetMessageId = m.id;
  contextTargetMessageText = m.text;
  contextTargetIsMine = m.sender === CURRENT_USER;

  const menu = document.getElementById('messageContextMenu');
  document.getElementById('ctxEditItem').style.display = contextTargetIsMine ? 'flex' : 'none';
  document.getElementById('ctxDeleteAllItem').style.display = contextTargetIsMine ? 'flex' : 'none';

  const x = e.clientX || window.innerWidth / 2;
  const y = e.clientY || window.innerHeight / 2;
  menu.style.left = Math.min(x, window.innerWidth - 190) + 'px';
  menu.style.top = Math.min(y, window.innerHeight - 250) + 'px';
  menu.style.display = 'block';
}

async function ctxAction(action) {
  const id = contextTargetMessageId;
  document.getElementById('messageContextMenu').style.display = 'none';

  if (action === 'copy') {
    navigator.clipboard.writeText(contextTargetMessageText);
    showToast('Message copied');
  } else if (action === 'reply') {
    startReply(id, contextTargetMessageText);
  } else if (action === 'forward') {
    forwardMessage(id);
  } else if (action === 'star') {
    await fetch(`/api/message/${id}/star`, { method: 'POST' });
    loadMessages(currentChatId, null, true);
  } else if (action === 'edit') {
    const newText = prompt('Edit message:', contextTargetMessageText);
    if (newText && newText.trim()) {
      await fetch(`/api/message/${id}/edit`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text: newText.trim() })
      });
    }
  } else if (action === 'delete_me') {
    await fetch(`/api/message/${id}/delete`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ mode: 'me' })
    });
    loadMessages(currentChatId, null, true);
  } else if (action === 'delete_everyone') {
    await fetch(`/api/message/${id}/delete`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ mode: 'everyone' })
    });
  }
}

function startReply(id, text) {
  replyTarget = { id, text };
  document.getElementById('replyBar').style.display = 'flex';
  document.getElementById('replyBarSender').textContent = 'Replying to message';
  document.getElementById('replyBarText').textContent = text.slice(0, 80);
  document.getElementById('messageInput').focus();
}
function cancelReply() {
  replyTarget = null;
  document.getElementById('replyBar').style.display = 'none';
}

async function forwardMessage(id) {
  const target = prompt('Forward to username:');
  if (!target) return;
  const res = await fetch('/api/chats/create', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ participant: target })
  });
  const data = await res.json();
  if (data.error) { showToast(data.error); return; }
  socket.emit('send_message', { chat_id: data.chat.id, text: contextTargetMessageText, forwarded: true });
  showToast('Message forwarded');
}

/* ---------------- AUDIO CALLING ---------------- */
function initCallHandlers() {
  // Incoming call
  socket.on('incoming_call', (data) => {
    if (data.chat_id === currentChatId) {
      pendingCallData = data;
      showIncomingCallUI(data.caller);
    } else {
      // Show toast notification for call from other chat
      showToast(`Incoming call from ${data.caller}`);
    }
  });

  // Call ringing
  socket.on('call_ringing', (data) => {
    updateCallStatus('Ringing...');
  });

  // Call connected
  socket.on('call_connected', (data) => {
    isCallActive = true;
    updateCallStatus('Connected');
    startCallTimer();
    showToast('Call connected');
  });

  // Call rejected
  socket.on('call_rejected', (data) => {
    endCallUI();
    showToast('Call rejected');
  });

  // Call ended
  socket.on('call_ended', (data) => {
    endCallUI();
    showToast('Call ended');
  });

  // Call failed
  socket.on('call_failed', (data) => {
    endCallUI();
    showToast(`Call failed: ${data.reason}`);
  });

  // WebRTC signaling
  socket.on('webrtc_offer', (data) => {
    handleWebRTCOffer(data);
  });

  socket.on('webrtc_answer', (data) => {
    handleWebRTCAnswer(data);
  });

  socket.on('webrtc_ice_candidate', (data) => {
    handleWebRTCIceCandidate(data);
  });
}

function startAudioCall() {
  if (!currentChat || currentChat.is_group) {
    showToast('Group calls are not supported yet');
    return;
  }

  const callee = currentChat.name;
  if (!callee) return;

  // Check if callee is online
  if (!currentChat.online) {
    showToast('User is offline');
    return;
  }

  isCaller = true;
  showCallUI(`Calling ${callee}...`, 'Connecting...');
  
  // Emit call request
  socket.emit('call_user', {
    chat_id: currentChatId,
    callee: callee
  });

  // Initialize WebRTC
  initWebRTC(true);
}

function showIncomingCallUI(caller) {
  const overlay = document.getElementById('callOverlay');
  document.getElementById('callAvatar').textContent = caller[0].toUpperCase();
  document.getElementById('callName').textContent = `Incoming call from ${caller}`;
  document.getElementById('callStatus').textContent = 'Incoming call...';
  document.getElementById('callTimer').style.display = 'none';
  document.getElementById('acceptCallBtn').style.display = 'flex';
  document.getElementById('muteBtn').style.display = 'none';
  overlay.classList.add('show');
}

function showCallUI(name, status) {
  const overlay = document.getElementById('callOverlay');
  document.getElementById('callAvatar').textContent = (name || '?')[0].toUpperCase();
  document.getElementById('callName').textContent = name || 'Calling...';
  document.getElementById('callStatus').textContent = status || 'Connecting...';
  document.getElementById('callTimer').style.display = 'none';
  document.getElementById('acceptCallBtn').style.display = 'none';
  document.getElementById('muteBtn').style.display = 'flex';
  overlay.classList.add('show');
}

function updateCallStatus(status) {
  document.getElementById('callStatus').textContent = status;
}

function endCallUI() {
  const overlay = document.getElementById('callOverlay');
  overlay.classList.remove('show');
  
  if (callTimerInterval) {
    clearInterval(callTimerInterval);
    callTimerInterval = null;
  }
  
  // Clean up WebRTC
  if (peerConnection) {
    peerConnection.close();
    peerConnection = null;
  }
  if (localStream) {
    localStream.getTracks().forEach(track => track.stop());
    localStream = null;
  }
  if (remoteStream) {
    remoteStream = null;
  }
  
  isCallActive = false;
  isCaller = false;
  callSeconds = 0;
  isMuted = false;
  pendingCallData = null;
}

function endCall() {
  if (isCallActive || isCaller || pendingCallData) {
    socket.emit('end_call', { chat_id: currentChatId });
  }
  endCallUI();
}

function acceptCall() {
  if (!pendingCallData) return;
  
  const overlay = document.getElementById('callOverlay');
  document.getElementById('acceptCallBtn').style.display = 'none';
  document.getElementById('muteBtn').style.display = 'flex';
  document.getElementById('callStatus').textContent = 'Connecting...';
  
  isCaller = false;
  isCallActive = true;
  
  // Accept the call
  socket.emit('accept_call', { chat_id: pendingCallData.chat_id });
  
  // Initialize WebRTC as callee
  initWebRTC(false);
  
  pendingCallData = null;
}

function toggleMute() {
  if (!localStream) return;
  isMuted = !isMuted;
  localStream.getAudioTracks().forEach(track => {
    track.enabled = !isMuted;
  });
  document.getElementById('muteBtn').textContent = isMuted ? '🔇' : '🎙️';
  document.getElementById('muteBtn').classList.toggle('muted', isMuted);
}

function startCallTimer() {
  callSeconds = 0;
  const timerEl = document.getElementById('callTimer');
  timerEl.style.display = 'block';
  
  if (callTimerInterval) clearInterval(callTimerInterval);
  
  callTimerInterval = setInterval(() => {
    callSeconds++;
    const mins = Math.floor(callSeconds / 60);
    const secs = callSeconds % 60;
    timerEl.textContent = `${mins.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
  }, 1000);
}

/* ---------------- WEBRTC IMPLEMENTATION ---------------- */
const ICE_SERVERS = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' }
  ]
};

async function initWebRTC(isCaller) {
  try {
    // Get user media
    localStream = await navigator.mediaDevices.getUserMedia({ 
      audio: true,
      video: false
    });
    
    // Create peer connection
    peerConnection = new RTCPeerConnection(ICE_SERVERS);
    
    // Add local stream tracks
    localStream.getTracks().forEach(track => {
      peerConnection.addTrack(track, localStream);
    });
    
    // Handle remote stream
    peerConnection.ontrack = (event) => {
      remoteStream = event.streams[0];
      // Play remote audio
      const audio = new Audio();
      audio.srcObject = remoteStream;
      audio.play().catch(e => console.error('Error playing audio:', e));
    };
    
    // Handle ICE candidates
    peerConnection.onicecandidate = (event) => {
      if (event.candidate) {
        socket.emit('webrtc_ice_candidate', {
          chat_id: currentChatId,
          candidate: event.candidate
        });
      }
    };
    
    // Connection state change
    peerConnection.onconnectionstatechange = () => {
      if (peerConnection.connectionState === 'disconnected' || 
          peerConnection.connectionState === 'failed') {
        endCall();
        showToast('Call disconnected');
      }
    };
    
    if (isCaller) {
      // Create offer
      const offer = await peerConnection.createOffer();
      await peerConnection.setLocalDescription(offer);
      
      socket.emit('webrtc_offer', {
        chat_id: currentChatId,
        offer: offer
      });
    }
  } catch (error) {
    console.error('WebRTC initialization error:', error);
    showToast('Could not start call: ' + error.message);
    endCall();
  }
}

async function handleWebRTCOffer(data) {
  if (!peerConnection) return;
  
  try {
    await peerConnection.setRemoteDescription(new RTCSessionDescription(data.offer));
    
    const answer = await peerConnection.createAnswer();
    await peerConnection.setLocalDescription(answer);
    
    socket.emit('webrtc_answer', {
      chat_id: currentChatId,
      answer: answer
    });
  } catch (error) {
    console.error('Error handling WebRTC offer:', error);
  }
}

async function handleWebRTCAnswer(data) {
  if (!peerConnection) return;
  
  try {
    await peerConnection.setRemoteDescription(new RTCSessionDescription(data.answer));
  } catch (error) {
    console.error('Error handling WebRTC answer:', error);
  }
}

async function handleWebRTCIceCandidate(data) {
  if (!peerConnection) return;
  
  try {
    await peerConnection.addIceCandidate(new RTCIceCandidate(data.candidate));
  } catch (error) {
    console.error('Error adding ICE candidate:', error);
  }
}

/* ---------------- EMOJI PICKER ---------------- */
function buildEmojiPicker() {
  const picker = document.getElementById('emojiPicker');
  picker.innerHTML = EMOJIS.map(e => `<span onclick="insertEmoji('${e}')">${e}</span>`).join('');
}
function toggleEmojiPicker() {
  const picker = document.getElementById('emojiPicker');
  picker.style.display = (picker.style.display === 'grid') ? 'none' : 'grid';
}
function insertEmoji(e) {
  const input = document.getElementById('messageInput');
  input.value += e;
  input.focus();
}

/* ---------------- CHAT FLAG ACTIONS ---------------- */
async function toggleCurrentChatFlag(kind) {
  if (!currentChatId) return;
  const map = { pin: 'pin', mute: 'mute', archive: 'archive' };
  await fetch(`/api/chats/${currentChatId}/${map[kind]}`, { method: 'POST' });
  await loadChats();
  showToast(kind + ' toggled');
}
async function deleteCurrentChat() {
  if (!currentChatId) return;
  if (!confirm('Delete this chat?')) return;
  await fetch(`/api/chats/${currentChatId}`, { method: 'DELETE' });
  showEmptyState();
  loadChats();
}

/* ---------------- PROFILE MODAL ---------------- */
async function openProfileModal() {
  const res = await fetch('/api/profile');
  const data = await res.json();
  document.getElementById('profileUsername').value = data.user.username;
  document.getElementById('profileAbout').value = data.user.about;
  const preview = document.getElementById('profileAvatarPreview');
  preview.innerHTML = data.user.avatar ? `<img src="${data.user.avatar}">` : data.user.username[0].toUpperCase();
  showModal('profileModal');
}
function handleAvatarUpload(e) {
  const file = e.target.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = () => {
    document.getElementById('profileAvatarPreview').innerHTML = `<img src="${reader.result}">`;
    document.getElementById('profileAvatarPreview').dataset.newAvatar = reader.result;
  };
  reader.readAsDataURL(file);
}
async function saveProfile() {
  const payload = {
    new_username: document.getElementById('profileUsername').value.trim(),
    about: document.getElementById('profileAbout').value.trim(),
    current_password: document.getElementById('profileCurrentPassword').value,
    new_password: document.getElementById('profileNewPassword').value,
  };
  const newAvatar = document.getElementById('profileAvatarPreview').dataset.newAvatar;
  if (newAvatar) payload.avatar = newAvatar;

  const res = await fetch('/api/profile', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
  });
  const data = await res.json();
  if (data.error) { showToast(data.error); return; }
  showToast('Profile updated');
  closeModal('profileModal');
  document.getElementById('myAvatar').innerHTML = data.user.avatar ? `<img src="${data.user.avatar}">` : data.user.username[0].toUpperCase();
  loadChats();
}

/* ---------------- NEW CHAT MODAL ---------------- */
function openNewChatModal() {
  document.getElementById('newChatSearch').value = '';
  document.getElementById('newChatUserList').innerHTML = '';
  showModal('newChatModal');
}
async function searchUsersForNewChat() {
  const q = document.getElementById('newChatSearch').value.trim();
  const res = await fetch('/api/users?q=' + encodeURIComponent(q));
  const data = await res.json();
  const list = document.getElementById('newChatUserList');
  list.innerHTML = data.users.map(u => `
    <div class="user-pick-item" onclick="startDirectChat('${u.username}')">
      <div class="avatar">${u.avatar ? `<img src="${u.avatar}">` : u.username[0].toUpperCase()}</div>
      <div><div>${escapeHtml(u.username)}</div><div style="font-size:12px;color:var(--text-secondary);">${escapeHtml(u.about || '')}</div></div>
    </div>`).join('') || '<div style="padding:12px;color:var(--text-secondary);">No users found</div>';
}
async function startDirectChat(username) {
  const res = await fetch('/api/chats/create', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ participant: username })
  });
  const data = await res.json();
  if (data.error) { showToast(data.error); return; }
  chats[data.chat.id] = data.chat;
  closeModal('newChatModal');
  renderChatList();
  openChat(data.chat.id);
}

/* ---------------- NEW GROUP MODAL ---------------- */
let selectedGroupMembers = new Set();
function openNewGroupModal() {
  selectedGroupMembers = new Set();
  document.getElementById('groupNameInput').value = '';
  document.getElementById('groupDescInput').value = '';
  document.getElementById('groupMemberSearch').value = '';
  document.getElementById('groupUserList').innerHTML = '';
  showModal('newGroupModal');
}
async function searchUsersForGroup() {
  const q = document.getElementById('groupMemberSearch').value.trim();
  const res = await fetch('/api/users?q=' + encodeURIComponent(q));
  const data = await res.json();
  const list = document.getElementById('groupUserList');
  list.innerHTML = data.users.map(u => `
    <div class="user-pick-item">
      <input type="checkbox" ${selectedGroupMembers.has(u.username) ? 'checked' : ''}
        onchange="toggleGroupMember('${u.username}', this.checked)">
      <div class="avatar">${u.avatar ? `<img src="${u.avatar}">` : u.username[0].toUpperCase()}</div>
      <div>${escapeHtml(u.username)}</div>
    </div>`).join('');
}
function toggleGroupMember(username, checked) {
  if (checked) selectedGroupMembers.add(username); else selectedGroupMembers.delete(username);
}
async function createGroup() {
  const name = document.getElementById('groupNameInput').value.trim();
  if (!name || selectedGroupMembers.size === 0) { showToast('Group name and at least 1 member required'); return; }
  const res = await fetch('/api/chats/create', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      is_group: true, group_name: name,
      group_description: document.getElementById('groupDescInput').value.trim(),
      members: Array.from(selectedGroupMembers)
    })
  });
  const data = await res.json();
  if (data.error) { showToast(data.error); return; }
  chats[data.chat.id] = data.chat;
  closeModal('newGroupModal');
  renderChatList();
  openChat(data.chat.id);
}

/* ---------------- GROUP INFO MODAL ---------------- */
function openGroupInfoIfGroup() {
  if (!currentChat || !currentChat.is_group) return;
  document.getElementById('groupInfoName').textContent = currentChat.name;
  document.getElementById('groupInfoDesc').textContent = currentChat.description || '';
  renderGroupMembersList();
  showModal('groupInfoModal');
}
function renderGroupMembersList() {
  const el = document.getElementById('groupMembersList');
  el.innerHTML = currentChat.members.map(m => `
    <div class="user-pick-item">
      <div class="avatar">${m[0].toUpperCase()}</div>
      <div style="flex:1;">${escapeHtml(m)} ${currentChat.admins.includes(m) ? '<span style="color:var(--accent);font-size:11px;">(admin)</span>' : ''}</div>
      ${m !== CURRENT_USER ? `<span style="cursor:pointer;color:var(--danger);" onclick="removeGroupMember('${m}')">Remove</span>` : ''}
    </div>`).join('');
}
async function addGroupMember() {
  const username = document.getElementById('groupAddInput').value.trim();
  if (!username) return;
  const res = await fetch(`/api/chats/${currentChatId}/group/add`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username })
  });
  const data = await res.json();
  if (data.error) { showToast(data.error); return; }
  currentChat = data.chat;
  chats[currentChatId] = data.chat;
  document.getElementById('groupAddInput').value = '';
  renderGroupMembersList();
  renderChatHeader();
}
async function removeGroupMember(username) {
  const res = await fetch(`/api/chats/${currentChatId}/group/remove`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username })
  });
  const data = await res.json();
  if (data.error) { showToast(data.error); return; }
  currentChat = data.chat;
  chats[currentChatId] = data.chat;
  renderGroupMembersList();
  renderChatHeader();
}

/* ---------------- STARRED MESSAGES ---------------- */
async function openStarredModal() {
  const res = await fetch('/api/messages/starred');
  const data = await res.json();
  const list = document.getElementById('starredList');
  list.innerHTML = data.messages.map(m => `
    <div style="padding:10px;border-bottom:1px solid var(--border);">
      <div style="font-size:12px;color:var(--accent);font-weight:600;">${escapeHtml(m.sender)}</div>
      <div style="font-size:14px;">${escapeHtml(m.text)}</div>
      <div style="font-size:11px;color:var(--text-secondary);">${formatTime(m.timestamp)}</div>
    </div>`).join('') || '<div style="padding:12px;color:var(--text-secondary);">No starred messages</div>';
  showModal('starredModal');
}

/* ---------------- GLOBAL SEARCH ---------------- */
async function handleGlobalSearch() {
  const q = document.getElementById('searchInput').value.trim();
  if (!q) return;
  const res = await fetch('/api/search?q=' + encodeURIComponent(q));
  const data = await res.json();
  const panel = document.getElementById('searchResultsPanel');
  let html = '';

  if (data.chats.length) {
    html += '<div class="search-section-title">Chats</div>';
    html += data.chats.map(c => `<div class="user-pick-item" onclick="closeModal('searchModal');openChat('${c.id}')">
      <div class="avatar">${(c.name||'?')[0].toUpperCase()}</div><div>${escapeHtml(c.name)}</div></div>`).join('');
  }
  if (data.users.length) {
    html += '<div class="search-section-title">Users</div>';
    html += data.users.map(u => `<div class="user-pick-item" onclick="closeModal('searchModal');startDirectChat('${u.username}')">
      <div class="avatar">${u.username[0].toUpperCase()}</div><div>${escapeHtml(u.username)}</div></div>`).join('');
  }
  if (data.messages.length) {
    html += '<div class="search-section-title">Messages</div>';
    html += data.messages.map(m => `<div class="user-pick-item" onclick="closeModal('searchModal');openChat('${m.chat_id}')">
      <div class="avatar">${m.sender[0].toUpperCase()}</div>
      <div><div style="font-weight:600;">${escapeHtml(m.sender)}</div><div style="font-size:12px;">${escapeHtml(m.text)}</div></div></div>`).join('');
  }
  panel.innerHTML = html || '<div style="padding:12px;color:var(--text-secondary);">No results found</div>';
  showModal('searchModal');
}
function focusInlineSearch() { document.getElementById('searchInput').focus(); }

/* ---------------- NOTIFICATIONS ---------------- */
function requestNotificationPermission() {
  if ('Notification' in window && Notification.permission === 'default') {
    Notification.requestPermission();
  }
}
function showDesktopNotification(m) {
  if (document.hasFocus()) return;
  const chat = chats[m.chat_id];
  if (chat && chat.muted) return;
  if ('Notification' in window && Notification.permission === 'granted') {
    const body = m.is_voice ? '🎤 Voice message' : m.text;
    new Notification(m.sender, { body: body });
  }
}
function playNotificationSound() {
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.type = 'sine';
    osc.frequency.setValueAtTime(880, ctx.currentTime);
    gain.gain.setValueAtTime(0.15, ctx.currentTime);
    gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.3);
    osc.connect(gain).connect(ctx.destination);
    osc.start();
    osc.stop(ctx.currentTime + 0.3);
  } catch (e) { /* audio not available */ }
}

/* ---------------- UTILITIES ---------------- */
function showModal(id) { document.getElementById(id).classList.add('show'); }
function closeModal(id) { document.getElementById(id).classList.remove('show'); }
function showToast(msg) {
  const toast = document.createElement('div');
  toast.className = 'toast';
  toast.textContent = msg;
  document.body.appendChild(toast);
  setTimeout(() => toast.remove(), 2500);
}
function escapeHtml(str) {
  if (!str) return '';
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}
function formatTime(iso) {
  if (!iso) return '';
  const d = new Date(iso);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}
function formatLastSeen(iso) {
  if (!iso) return 'offline';
  const d = new Date(iso);
  return 'last seen ' + d.toLocaleString([], { hour: '2-digit', minute: '2-digit', month: 'short', day: 'numeric' });
}
</script>
</body>
</html>
"""

# --------------------------------------------------------------------------
# Server startup
# --------------------------------------------------------------------------
if __name__ == "__main__":
    print("[NovaChat] Starting server on http://0.0.0.0:5000 ...")
    socketio.run(app, host="0.0.0.0", port=5000, debug=True , allow_unsafe_werkzeug=True) 