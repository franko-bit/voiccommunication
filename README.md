# Voice Chat Pro

A browser voice-chat app using WebRTC for audio and WebSockets for signaling.

## Run locally

```bash
npm install
npm start
```

Open http://localhost:10000 and allow microphone access.

The frontend files are `index.html`, `starfield.css`, and `orbit-comms.js`.

## Deploy to Render

Create a **Web Service** connected to this repository:

- Build command: `npm install`
- Start command: `npm start`
- Environment variable: Render supplies `PORT` automatically

The app uses the same Render HTTPS origin for both the website and WebSocket signaling. The browser automatically selects `wss://` on the deployed HTTPS site.

After deployment, open the public Render URL, create a room, and share the invite link. WebRTC audio may require a TURN service for users on restrictive networks; the app currently uses public Google STUN servers.
