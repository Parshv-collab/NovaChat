# 💬 NovaChat

A **real-time messaging web application** built with Flask, SocketIO, and MongoDB.  
Inspired by WhatsApp, with features like **voice messages**, **groups**, **read receipts**, and **message replies**.

---

## ✨ Features

- **Real-time messaging** (WebSocket via SocketIO)
- **Voice messages** (record and send audio)
- **Group chats** (create and manage groups)
- **Direct messaging** (1-on-1 chats)
- **Read receipts** (✓ and ✓✓)
- **Typing indicators**
- **Reply to messages**
- **Forward messages**
- **Star messages**
- **Pin / Mute / Archive chats**
- **Search** (chats, users, and messages)
- **Dark/Light theme**
- **User profiles** (avatar, about, username)
- **Online/Offline status**

---

## 🛠 Tech Stack

- **Backend:** Flask, Flask-SocketIO
- **Database:** MongoDB
- **Real-time:** SocketIO
- **Authentication:** bcrypt + sessions
- **Voice:** MediaRecorder API (Web Audio)

---

## 🚀 Getting Started

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/NovaChat.git
cd NovaChat
```

### 2. Install dependencies
```bash
pip install flask flask-socketio pymongo bcrypt python-dotenv
```

### 3. Set up MongoDB
- Create a free MongoDB Atlas account or run locally.
- Copy your connection string (MONGO_URI).

### 4. Create a `.env` file
```
MONGO_URI=mongodb://localhost:27017/
SECRET_KEY=your_secret_key_here
```

### 5. Run the app
```bash
python app.py
```

### 6. Open your browser
```
http://localhost:5000
```

---

## 🧪 Demo Accounts

All demo accounts have the password: `password123`

- `alice`
- `bob`
- `charlie`
- `diana`
- `eve`

---


## 🧠 Lessons Learned

- Building a real-time app with Flask-SocketIO
- Handling voice recording and playback in the browser
- Managing state with MongoDB
- Creating a responsive, dark/light UI

---
## 📄 License

MIT

---

## 🙏 Credits

Built by **Parshv Ashok Chandaria** — 14-year-old developer from Mumbai.