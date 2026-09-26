import { AccessToken } from 'livekit-server-sdk'
import 'dotenv/config'

export async function createRoomToken({ roomName, userId, userName }) {
  const token = new AccessToken(process.env.LIVEKIT_API_KEY, process.env.LIVEKIT_API_SECRET, {
    identity: userId,
    name: userName,
  })
  token.addGrant({
    room: roomName,
    roomJoin: true,
    canPublish: true,
    canSubscribe: true,
  })
  return await token.toJwt()
}