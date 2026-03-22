"""
Echo World — Auto Daily Earning + Push Notification System
Railway.app এ deploy করো
প্রতিদিন রাত ১২:০১ AM (Bangladesh time) automatic চলবে
"""

import os
import requests
from datetime import datetime, timezone, timedelta
from supabase import create_client, Client
from apscheduler.schedulers.blocking import BlockingScheduler

# ── Config ──
SUPABASE_URL     = os.environ.get('SUPABASE_URL')
SUPABASE_KEY     = os.environ.get('SUPABASE_SERVICE_KEY')
ONESIGNAL_APP_ID = os.environ.get('ONESIGNAL_APP_ID')
ONESIGNAL_KEY    = os.environ.get('ONESIGNAL_REST_KEY')

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
BD_TZ = timezone(timedelta(hours=6))

def get_today_bd():
    return datetime.now(BD_TZ).strftime('%Y-%m-%d')

def send_push(user_id: str, title: str, message: str):
    """OneSignal দিয়ে mobile push notification পাঠাও"""
    if not ONESIGNAL_APP_ID or not ONESIGNAL_KEY:
        return
    try:
        requests.post(
            'https://onesignal.com/api/v1/notifications',
            headers={
                'Authorization': f'Basic {ONESIGNAL_KEY}',
                'Content-Type': 'application/json',
            },
            json={
                'app_id': ONESIGNAL_APP_ID,
                'filters': [{'field': 'tag', 'key': 'user_id', 'relation': '=', 'value': user_id}],
                'headings': {'en': title},
                'contents': {'en': message},
                'small_icon': 'ic_stat_onesignal_default',
            },
            timeout=10
        )
    except Exception as e:
        print(f'  Push error: {e}')

def send_db_notif(user_id: str, message: str):
    """Supabase notifications table এ notification insert করো"""
    try:
        supabase.table('notifications').insert({
            'user_id': user_id,
            'from_user_id': None,
            'type': 'system',
            'message': f'🌐 Echo World: {message}',
            'read': False,
        }).execute()
    except Exception as e:
        print(f'  DB notif error: {e}')

def notify(user_id: str, title: str, message: str):
    """Push notification + DB notification দুটোই পাঠাও"""
    send_push(user_id, title, message)
    send_db_notif(user_id, message)

def process_daily_earnings():
    """
    প্রতিদিন রাত ১২:০১ AM এ চলবে:
    1. আজ যে post করেছে তাদের income দাও
    2. Referral commission দাও
    3. Push notification পাঠাও
    """
    today    = get_today_bd()
    now_iso  = datetime.now(BD_TZ).isoformat()

    print(f'\n{"="*50}')
    print(f'Echo World Daily Earning — {today}')
    print(f'{"="*50}')

    # ── আজ কে post করেছে ──
    try:
        posts_resp = supabase.table('posts').select('user_id').gte(
            'created_at', f'{today}T00:00:00+06:00'
        ).lte(
            'created_at', f'{today}T23:59:59+06:00'
        ).execute()
        posted_today = set(p['user_id'] for p in (posts_resp.data or []))
        print(f'আজ post করেছে: {len(posted_today)} জন')
    except Exception as e:
        print(f'Posts error: {e}'); return

    # ── Active accounts ──
    try:
        accs_resp = supabase.table('investment_accounts').select(
            'user_id, wallet_balance, total_earned, referred_by, status'
        ).eq('status', 'active').execute()
        accounts = {a['user_id']: a for a in (accs_resp.data or [])}
    except Exception as e:
        print(f'Accounts error: {e}'); return

    # ── Active investments ──
    try:
        inv_resp = supabase.table('investments').select(
            'user_id, amount_usd, daily_rate, end_date, status'
        ).eq('status', 'active').execute()
        investments = {}
        for inv in (inv_resp.data or []):
            uid = inv['user_id']
            if uid not in investments:
                investments[uid] = []
            investments[uid].append(inv)
    except Exception as e:
        print(f'Investments error: {e}'); return

    # ── প্রতিটা user process ──
    total_paid  = 0
    total_users = 0
    referral_q  = {}  # referrer_id → amount

    for uid in posted_today:
        if uid not in investments:
            continue
        acc = accounts.get(uid)
        if not acc:
            continue

        # Daily earning calculate
        daily_total = 0.0
        for inv in investments[uid]:
            if inv.get('end_date') and inv['end_date'] < today:
                continue
            amount = float(inv.get('amount_usd') or 0)
            rate   = float(inv.get('daily_rate')  or 0)
            daily_total += round(amount * rate / 100, 4)

        if daily_total <= 0:
            continue

        # Daily earning record
        try:
            # আগে আজকের earning আছে কিনা check
            existing = supabase.table('daily_earnings').select('id').eq(
                'user_id', uid).eq('date', today).eq('type', 'daily').execute()
            if existing.data:
                print(f'  ⏭ {uid[:8]}... already paid today')
                continue

            supabase.table('daily_earnings').insert({
                'user_id': uid,
                'investment_id': None,
                'amount': daily_total,
                'type': 'daily',
                'date': today,
                'note': 'Auto daily earning — post verified',
            }).execute()
        except Exception as e:
            print(f'  earning insert error: {e}'); continue

        # Wallet update
        new_balance = round(float(acc.get('wallet_balance') or 0) + daily_total, 4)
        new_earned  = round(float(acc.get('total_earned')   or 0) + daily_total, 4)

        try:
            supabase.table('investment_accounts').update({
                'wallet_balance': new_balance,
                'total_earned':   new_earned,
            }).eq('user_id', uid).execute()
        except Exception as e:
            print(f'  wallet update error: {e}'); continue

        total_paid  += daily_total
        total_users += 1
        print(f'  ✅ {uid[:8]}... → +${daily_total:.4f}')

        # Push + DB notification
        notify(
            uid,
            '💰 Echo World — Daily Earning',
            f'✅ আজকের আয় ${daily_total:.2f} wallet এ যোগ হয়েছে! Keep posting daily!'
        )

        # Referral commission queue
        referrer_id = acc.get('referred_by')
        if referrer_id:
            l1 = round(daily_total * 0.50, 4)
            referral_q[referrer_id] = referral_q.get(referrer_id, 0) + l1

            l1_acc = accounts.get(referrer_id)
            if l1_acc:
                l2 = l1_acc.get('referred_by')
                if l2:
                    l2_amt = round(daily_total * 0.25, 4)
                    referral_q[l2] = referral_q.get(l2, 0) + l2_amt

    # ── Referral commissions ──
    print(f'\nReferral commissions: {len(referral_q)} জন')
    for referrer_id, commission in referral_q.items():
        if commission <= 0:
            continue
        ref_acc = accounts.get(referrer_id)
        if not ref_acc:
            continue

        new_bal = round(float(ref_acc.get('wallet_balance') or 0) + commission, 4)
        new_ern = round(float(ref_acc.get('total_earned')   or 0) + commission, 4)

        try:
            supabase.table('investment_accounts').update({
                'wallet_balance': new_bal,
                'total_earned':   new_ern,
            }).eq('user_id', referrer_id).execute()

            supabase.table('daily_earnings').insert({
                'user_id': referrer_id,
                'investment_id': None,
                'amount': commission,
                'type': 'referral',
                'date': today,
                'note': 'Auto referral commission',
            }).execute()

            notify(
                referrer_id,
                '🔗 Echo World — Referral Commission',
                f'🔗 Referral commission ${commission:.2f} wallet এ যোগ হয়েছে!'
            )
            print(f'  💰 {referrer_id[:8]}... → +${commission:.4f}')
        except Exception as e:
            print(f'  referral error: {e}')

    # ── Summary ──
    print(f'\n{"="*50}')
    print(f'✅ {total_users} জনকে মোট ${total_paid:.4f} দেওয়া হয়েছে')
    print(f'{"="*50}\n')


def send_withdraw_notification():
    """14th and 28th of every month — withdrawal window notification"""
    print('Sending withdrawal window notifications...')
    try:
        accs = supabase.table('investment_accounts').select('user_id').eq('status', 'active').execute()
        for acc in (accs.data or []):
            notify(
                acc['user_id'],
                '📅 Echo World — Withdrawal Window Open',
                '✅ Today is withdrawal day! You can withdraw your earnings now. Go to Invest → Withdraw.'
            )
        print(f'  ✅ Sent to {len(accs.data or [])} users')
    except Exception as e:
        print(f'Withdraw notification error: {e}')

def main():
    print('Echo World Auto Earning System starting...')
    print(f'BD Time: {datetime.now(BD_TZ).strftime("%Y-%m-%d %H:%M:%S")}')

    scheduler = BlockingScheduler(timezone='Asia/Dhaka')

    # Daily earning — রাত ১২:০১ AM
    scheduler.add_job(
        process_daily_earnings,
        'cron',
        hour=0,
        minute=1,
        id='daily_earning',
    )

    # Withdrawal notification — 14th and 28th at 8 AM BD time
    scheduler.add_job(
        send_withdraw_notification,
        'cron',
        day='14,28',
        hour=8,
        minute=0,
        id='withdraw_notification',
    )

    print('✅ Scheduler ready — প্রতিদিন রাত ১২:০১ AM (BD) এ চলবে')
    print('✅ Withdrawal notification — 14th & 28th at 8 AM (BD)')

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print('Stopped.')

if __name__ == '__main__':
    main()
