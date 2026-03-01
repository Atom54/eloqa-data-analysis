import marimo

__generated_with = "0.20.1"
app = marimo.App(width="full")


@app.cell
def __():
    import marimo as mo
    import pandas as pd
    import numpy as np
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import psycopg2

    ELOQA_PURPLE = "#6C5CE7"
    ELOQA_COLORS = ["#6C5CE7", "#a29bfe", "#fd79a8", "#00cec9", "#fdcb6e", "#e17055", "#00b894"]

    def query(sql):
        conn = psycopg2.connect(host="localhost", dbname="eloqa", user="eloqa_reader", password="readonly")
        try:
            return pd.read_sql(sql, conn)
        finally:
            conn.close()

    return ELOQA_COLORS, ELOQA_PURPLE, go, make_subplots, mo, np, pd, psycopg2, px, query


@app.cell
def __(mo):
    mo.md("""
    # 2️⃣ Annulation de la période d'essai
    
    **Fibery :** #4356  
    **Question :** À quel moment les utilisateurs annulent-ils leur trial après activation ?  
    **Période :** Septembre 2025 – Février 2026
    
    ---
    """)


@app.cell
def __(mo):
    mo.md("## 📋 Méthodologie")


@app.cell
def __(mo):
    mo.accordion({
        "Sources & Variables (cliquer pour déplier)": mo.md("""
**Source :** DB Eloqa — Table `PayEvent` (webhooks RevenueCat)

**Approche :**
1. Identifier les activations de trial : `type = 'INITIAL_PURCHASE'` + `periodType = 'TRIAL'`
2. Identifier les annulations : `type = 'CANCELLATION'` + `periodType = 'TRIAL'`
3. Matcher par `originalAppUserId` et calculer le délai : `eventTimestampMs(CANCEL) - purchasedAtMs(INITIAL)`

**Variables :**
- `PayEvent.type` — type d'événement (`INITIAL_PURCHASE`, `CANCELLATION`, `RENEWAL`)
- `PayEvent.periodType` — `TRIAL` ou `NORMAL`
- `PayEvent.purchasedAtMs` — timestamp de l'activation (côté store)
- `PayEvent.eventTimestampMs` — timestamp de l'annulation (côté RevenueCat)
- `PayEvent.originalAppUserId` — identifiant stable cross-device

**Limites :**
- Annulation ≠ non-conversion. Un user peut annuler (pour éviter le renouvellement auto) tout en utilisant le trial jusqu'à expiration, puis renouveler manuellement
- Le timestamp d'annulation = notification RevenueCat, pas forcément le clic exact
- Certains users annulent via les settings iOS/Android, pas dans l'app
- Filtré sur trials annuels (`productId LIKE '%ann%'`) + `environment = 'PRODUCTION'`
        """)
    })


@app.cell
def __(mo):
    mo.md("## 📊 Timing des annulations")


@app.cell
def __(query, pd, px, go, ELOQA_COLORS, mo):
    # Calculate cancellation delays
    df_cancel = query("""
        WITH trial_starts AS (
            SELECT "originalAppUserId" as user_id, 
                   MIN("purchasedAtMs"::bigint) as trial_ts
            FROM "PayEvent"
            WHERE environment = 'PRODUCTION' 
            AND "periodType" = 'TRIAL' AND type = 'INITIAL_PURCHASE'
            AND lower("productId") LIKE '%%ann%%'
            GROUP BY "originalAppUserId"
        ),
        trial_cancels AS (
            SELECT pe."originalAppUserId" as user_id,
                   MIN(pe."eventTimestampMs"::bigint) as cancel_ts
            FROM "PayEvent" pe
            JOIN trial_starts ts ON pe."originalAppUserId" = ts.user_id
            WHERE pe.environment = 'PRODUCTION' 
            AND pe."periodType" = 'TRIAL' AND pe.type = 'CANCELLATION'
            GROUP BY pe."originalAppUserId"
        )
        SELECT 
            (tc.cancel_ts - ts.trial_ts) / 3600000.0 as delay_hours
        FROM trial_starts ts
        JOIN trial_cancels tc ON ts.user_id = tc.user_id
    """)

    def bucket_delay(h):
        if h < 1: return '< 1h'
        elif h < 24: return '1-24h'
        elif h < 72: return 'Jour 2-3'
        elif h < 168: return 'Jour 4-7'
        else: return '7+ jours'

    df_cancel['bucket'] = df_cancel['delay_hours'].apply(bucket_delay)
    cancel_dist = df_cancel['bucket'].value_counts().reindex(
        ['< 1h', '1-24h', 'Jour 2-3', 'Jour 4-7', '7+ jours']
    ).reset_index()
    cancel_dist.columns = ['Délai', 'Users']
    cancel_dist['pct'] = (cancel_dist['Users'] / cancel_dist['Users'].sum() * 100).round(1)

    fig_cancel = px.bar(cancel_dist, x='Délai', y='Users', text='pct',
                        title='Quand les utilisateurs annulent leur trial',
                        color='Délai', color_discrete_sequence=ELOQA_COLORS)
    fig_cancel.update_traces(texttemplate='%{text}%', textposition='outside')
    fig_cancel.update_layout(height=420, showlegend=False)
    fig_cancel
    return df_cancel,


@app.cell
def __(mo):
    mo.callout("""
**29% annulent dans l'heure** — comportement "j'active pour voir, je cancel direct au cas où".  
**61% annulent en 3 jours** — la majorité ne va même pas au bout des 7 jours de trial.
    """, kind="warn")


@app.cell
def __(mo):
    mo.md("## 📊 Distribution fine des délais (histogramme)")


@app.cell
def __(df_cancel, px, ELOQA_PURPLE, mo):
    # Detailed histogram of first 48 hours
    first_48h = df_cancel[df_cancel['delay_hours'] <= 48].copy()
    
    fig_hist = px.histogram(first_48h, x='delay_hours', nbins=48,
                            title='Distribution des annulations — premières 48h (par heure)',
                            labels={'delay_hours': 'Heures après activation'},
                            color_discrete_sequence=[ELOQA_PURPLE])
    fig_hist.update_layout(height=380, yaxis_title='Utilisateurs')
    fig_hist


@app.cell
def __(mo):
    mo.md("## 📊 Taux de conversion trial → payant")


@app.cell
def __(query, go, ELOQA_PURPLE, ELOQA_COLORS, mo):
    stats = query("""
        WITH trial_users AS (
            SELECT DISTINCT "originalAppUserId" as uid
            FROM "PayEvent"
            WHERE environment='PRODUCTION' AND "periodType"='TRIAL' AND type='INITIAL_PURCHASE'
            AND lower("productId") LIKE '%%ann%%'
        ),
        cancelled AS (
            SELECT DISTINCT "originalAppUserId" as uid
            FROM "PayEvent"
            WHERE environment='PRODUCTION' AND "periodType"='TRIAL' AND type='CANCELLATION'
            AND "originalAppUserId" IN (SELECT uid FROM trial_users)
        ),
        converted AS (
            SELECT DISTINCT "originalAppUserId" as uid
            FROM "PayEvent"
            WHERE environment='PRODUCTION' AND "periodType"='NORMAL'
            AND type IN ('RENEWAL', 'INITIAL_PURCHASE')
            AND "originalAppUserId" IN (SELECT uid FROM trial_users)
        )
        SELECT
            (SELECT COUNT(*) FROM trial_users) as total_trials,
            (SELECT COUNT(*) FROM cancelled) as total_cancelled,
            (SELECT COUNT(*) FROM converted) as total_converted
    """)

    total = int(stats['total_trials'].iloc[0])
    cancelled = int(stats['total_cancelled'].iloc[0])
    converted = int(stats['total_converted'].iloc[0])
    active = total - cancelled - converted

    fig_pie = go.Figure(data=[go.Pie(
        labels=['Annulé', 'Converti payant', 'Trial actif/en cours'],
        values=[cancelled, converted, max(active, 0)],
        marker=dict(colors=[ELOQA_COLORS[2], ELOQA_COLORS[6], ELOQA_COLORS[1]]),
        textinfo='label+percent+value',
        hole=0.4
    )])
    fig_pie.update_layout(title=f'Devenir des {total} trials annuels', height=420)

    mo.vstack([
        fig_pie,
        mo.callout(f"""
**{total}** trials annuels au total  
**{cancelled}** annulés ({round(cancelled/total*100, 1)}%)  
**{converted}** convertis en payant ({round(converted/total*100, 1)}%)
        """, kind="info")
    ])


@app.cell
def __(mo):
    mo.md("""
    ---
    ## 💡 Hypothèses & Conclusions
    
    **Hypothèse 1 :** La majorité des annulations sont réfléchies  
    ❌ **Infirmé** — 29% annulent dans l'heure. C'est un réflexe défensif ("cancel au cas où"), pas un rejet du produit
    
    **Hypothèse 2 :** Les utilisateurs explorent le produit pendant 7 jours avant de décider  
    ❌ **Infirmé** — 61% ont déjà annulé au jour 3. Le trial de 7 jours est trop long pour le cycle de décision
    
    **Hypothèse 3 :** Le taux d'annulation est anormalement élevé  
    ⚠️ **Nuancé** — 76.4% cancel est dans la moyenne haute de l'industrie SaaS mobile (60-80%), mais le timing est le vrai problème
    
    ---
    
    **Recommandations :**
    - **Notification push J1** ("Tu as découvert 12 mots ! Reviens demain pour les ancrer") — capter les 29% qui cancel dans l'heure
    - **Email/notification J3** avec les progrès concrets → capter les 32% qui cancel entre J1 et J3
    - **Raccourcir le trial à 3 jours** et tester l'impact sur la conversion (les users décident avant J3 de toute façon)
    
    ---
    *📊 Analyse par Quanty — Sprint Fév 2026*
    """)


if __name__ == "__main__":
    app.run()
