import { createClient } from '@supabase/supabase-js'
import 'dotenv/config'

// service role key — bypasses RLS entirely, this file must never run in a browser
export const supabaseAdmin = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)