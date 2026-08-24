const joinScreen = document.getElementById("join-screen");
const chatScreen = document.getElementById("chat-screen");

const usernameInput = document.getElementById("username-input");
const joinBtn = document.getElementById("join-btn");
const joinError = document.getElementById("join-error");

const messagesEl = document.getElementById("messages");
const messageForm = document.getElementById("message-form");
const messageInput = document.getElementById("message-input");

const userListEl = document.getElementById("user-list");
const userCountEl = document.getElementById("user-count");

const meLabel = document.getElementById("me-label");
const connStatus = document.getElementById("conn-status");

const typingIndicator = document.getElementById("typing-indicator");

let socket = null;
let myUsername = null;
let typingTimeout = null;
let reconnectTimeout = null;
let lastTypingSent = 0;


/* ---------- WebSocket URL ---------- */

function wsUrl() {

    const protocol =
        location.protocol === "https:"
            ? "wss:"
            : "ws:";

    return `${protocol}//${location.host}/ws`;
}


/* ---------- Connect ---------- */

function connect(username) {

    clearTimeout(reconnectTimeout);

    socket = new WebSocket(wsUrl());


    socket.addEventListener("open", () => {

        connStatus.textContent = "● connected";
        connStatus.classList.remove("offline");

        socket.send(
            JSON.stringify({
                type: "join",
                username: username
            })
        );
    });


    socket.addEventListener("message", (event) => {

        try {

            const msg = JSON.parse(event.data);

            handleServerMessage(msg);

        } catch (error) {

            console.error(
                "Invalid server message:",
                error
            );
        }
    });


    socket.addEventListener("close", () => {

        connStatus.textContent =
            "● disconnected — retrying…";

        connStatus.classList.add("offline");

        reconnectTimeout = setTimeout(() => {

            if (myUsername) {
                connect(myUsername);
            }

        }, 2000);
    });


    socket.addEventListener("error", () => {

        console.error("WebSocket error");

        socket.close();
    });
}


/* ---------- Server Messages ---------- */

function handleServerMessage(msg) {

    switch (msg.type) {

        case "message":
            renderMessage(msg);
            break;

        case "history":

            msg.messages.forEach(renderMessage);

            break;

        case "notice":
            renderNotice(msg.text);
            break;

        case "error":
            renderNotice(`⚠ ${msg.text}`);
            break;

        case "userlist":
            renderUserList(msg.users);
            break;

        case "typing":
            showTyping(msg.username);
            break;

        default:

            console.warn(
                "Unknown server message:",
                msg
            );
    }
}


/* ---------- Render Message ---------- */

function renderMessage(msg) {

    const div = document.createElement("div");

    const isOwn =
        msg.username === myUsername;

    div.className =
        "msg" + (isOwn ? " own" : "");


    const meta = document.createElement("div");

    meta.className = "meta";

    meta.textContent =
        `${isOwn ? "You" : msg.username} · ${formatTime(msg.timestamp)}`;

    meta.appendChild(signatureBadge(msg));


    const text = document.createElement("div");

    text.className = "text";

    text.textContent = msg.text;


    if (msg.tampered) {

        div.classList.add("tampered");
    }


    div.appendChild(meta);
    div.appendChild(text);

    messagesEl.appendChild(div);

    scrollToBottom();
}


/* ---------- Signature Badge ---------- */

function signatureBadge(msg) {

    const badge = document.createElement("span");

    if (msg.tampered) {

        badge.className =
            "badge badge-tampered";

        badge.textContent =
            "⚠ tampered";

    } else if (msg.signature_valid) {

        badge.className =
            "badge badge-verified";

        badge.textContent =
            "✓ verified";

    } else {

        badge.className =
            "badge badge-unverified";

        badge.textContent =
            "✗ unverified";
    }

    return badge;
}


/* ---------- Notice ---------- */

function renderNotice(text) {

    const div = document.createElement("div");

    div.className = "notice";

    div.textContent = text;

    messagesEl.appendChild(div);

    scrollToBottom();
}


/* ---------- Online Users ---------- */

function renderUserList(users) {

    userListEl.innerHTML = "";

    userCountEl.textContent =
        users.length;


    users.forEach((username) => {

        const li =
            document.createElement("li");

        li.textContent =
            username === myUsername
                ? `${username} (you)`
                : username;


        if (username === myUsername) {

            li.classList.add("me");
        }


        userListEl.appendChild(li);
    });
}


/* ---------- Typing ---------- */

function showTyping(username) {

    if (username === myUsername) {
        return;
    }

    typingIndicator.textContent =
        `${username} is typing…`;

    clearTimeout(typingTimeout);

    typingTimeout = setTimeout(() => {

        typingIndicator.textContent = "";

    }, 1500);
}


/* ---------- Time ---------- */

function formatTime(timestamp) {

    const date =
        new Date(timestamp);

    return date.toLocaleTimeString(
        [],
        {
            hour: "2-digit",
            minute: "2-digit"
        }
    );
}


/* ---------- Scroll ---------- */

function scrollToBottom() {

    messagesEl.scrollTop =
        messagesEl.scrollHeight;
}


/* ---------- Join ---------- */

function doJoin() {

    const name =
        usernameInput.value.trim();


    if (!name) {

        joinError.textContent =
            "Please enter a username.";

        return;
    }


    joinError.textContent = "";

    myUsername = name;

    meLabel.textContent =
        `You are: ${name}`;


    joinScreen.classList.add("hidden");

    chatScreen.classList.remove("hidden");


    connect(name);

    messageInput.focus();
}


joinBtn.addEventListener(
    "click",
    doJoin
);


usernameInput.addEventListener(
    "keydown",
    (event) => {

        if (event.key === "Enter") {

            doJoin();
        }
    }
);


/* ---------- Send Message ---------- */

messageForm.addEventListener(
    "submit",
    (event) => {

        event.preventDefault();


        const text =
            messageInput.value.trim();


        if (
            !text ||
            !socket ||
            socket.readyState !== WebSocket.OPEN
        ) {

            return;
        }


        const timestamp = Date.now();


        /*
         * No signMessage() here.
         *
         * The server now:
         * 1. Signs the message using ECDSA
         * 2. Verifies the signature
         * 3. Encrypts the message
         * 4. Creates the integrity hash
         * 5. Stores it in SQLite
         */

        socket.send(
            JSON.stringify({
                type: "message",
                text: text,
                timestamp: timestamp
            })
        );


        messageInput.value = "";

        typingIndicator.textContent = "";
    }
);


/* ---------- Typing Indicator ---------- */

messageInput.addEventListener(
    "input",
    () => {

        const now = Date.now();


        if (
            now - lastTypingSent > 800 &&
            socket &&
            socket.readyState === WebSocket.OPEN
        ) {

            socket.send(
                JSON.stringify({
                    type: "typing"
                })
            );

            lastTypingSent = now;
        }
    }
);