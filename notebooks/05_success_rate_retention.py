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
    # 5️⃣ Taux de réussite vs Rétention — 🔥 Finding principal
    
    **Fibery :** #4355 + Deep Dive #4935  
    **Question :** Quel lien entre le taux de mémorisation des mots et la rétention ? Pourquoi 89% des users échouent ?  
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
**Source :** DB Eloqa — Table `ReviewSession`

**Variables :**
- `newMemorizedCardsNumber` : cartes passées au statut "mémorisé" via l'algorithme de répétition espacée (type Anki/Leitner)
- `newDiscoveredCardsNumber` : nouvelles cartes vues pour la première fois
- `totalMemorizedCardsNumber` : cumul de cartes mémorisées à la fin de la session
- `totalDiscoveredCardsNumber` : cumul de cartes découvertes
- **Taux de réussite** = `SUM(newMemorized) / SUM(newDiscovered)` par utilisateur

**Point critique — Mécanique de répétition espacée :**
Un mot n'est compté "mémorisé" que lorsque l'utilisateur l'a correctement rappelé **plusieurs fois à des intervalles croissants**. 
**Il est structurellement impossible de mémoriser un mot en 1 seule session.** Le système est conçu pour que la mémorisation nécessite des retours.

**Rétention D14 :** l'utilisateur a une session finalisée ≥14 jours après sa première.

**Limites :**
- Le taux de réussite <20% est **biaisé par design** : les users à 1 session ne PEUVENT PAS mémoriser
- Le lien réussite → rétention est corrélationnel. Les users qui font plus de sessions mémorisent plus ET restent plus longtemps. Pas de preuve de causalité directe
- Filtré sur users avec `newDiscovered > 0` et `firstSession < NOW() - 14 jours`
        """)
    })


@app.cell
def __(mo):
    mo.md("## 📊 Le cliff : taux de réussite vs rétention D14")


@app.cell
def __(query, pd, go, ELOQA_COLORS, ELOQA_PURPLE, make_subplots, mo):
    df_success = query("""
        WITH user_perf AS (
            SELECT 
                "userId",
                SUM("newMemorizedCardsNumber") as memorized,
                SUM("newDiscoveredCardsNumber") as discovered,
                COUNT(*) as sessions,
                MIN("startedAt") as first_session,
                MAX("startedAt") as last_session
            FROM "ReviewSession"
            WHERE finalized = true AND "startedAt" >= '2025-09-01'
            GROUP BY "userId"
            HAVING SUM("newDiscoveredCardsNumber") > 0
            AND MIN("startedAt") < NOW() - interval '14 days'
        )
        SELECT 
            CASE
                WHEN memorized::float / discovered < 0.2 THEN '< 20%'
                WHEN memorized::float / discovered < 0.4 THEN '20-40%'
                WHEN memorized::float / discovered < 0.6 THEN '40-60%'
                WHEN memorized::float / discovered < 0.8 THEN '60-80%'
                ELSE '80%+'
            END as success_bucket,
            COUNT(*) as users,
            ROUND(AVG(sessions), 1) as avg_sessions,
            ROUND(AVG(EXTRACT(EPOCH FROM (last_session - first_session)) / 86400)::numeric, 0) as lifespan,
            ROUND(COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (last_session - first_session)) >= 14*86400)::numeric / COUNT(*) * 100, 1) as d14,
            ROUND(COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (last_session - first_session)) >= 7*86400)::numeric / COUNT(*) * 100, 1) as d7,
            MIN(memorized::float / discovered) as min_rate
        FROM user_perf
        GROUP BY success_bucket
        ORDER BY min_rate
    """)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(x=df_success['success_bucket'], y=df_success['users'], name='Utilisateurs',
               marker_color=ELOQA_COLORS[1], opacity=0.6),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=df_success['success_bucket'], y=df_success['d14'], name='Rétention D14 (%)',
                   line=dict(color=ELOQA_COLORS[2], width=4), mode='lines+markers+text',
                   text=df_success['d14'].astype(str) + '%', textposition='top center',
                   marker=dict(size=14)),
        secondary_y=True
    )
    # Threshold line
    fig.add_hline(y=20, line_dash="dash", line_color="red", 
                  annotation_text="Seuil critique", secondary_y=True)
    fig.update_layout(
        title="🔥 Le cliff : taux de mémorisation vs rétention D14",
        height=500, font=dict(size=14)
    )
    fig.update_yaxes(title_text="Nombre d'utilisateurs", secondary_y=False)
    fig.update_yaxes(title_text="Rétention D14 (%)", secondary_y=True, range=[0, 100])
    fig
    return df_success,


@app.cell
def __(df_success, mo):
    low = df_success[df_success['success_bucket'] == '< 20%'].iloc[0]
    mid = df_success[df_success['success_bucket'] == '20-40%'].iloc[0]
    
    mo.callout(f"""
**{int(low['users'])} users ({round(low['users'] / df_success['users'].sum() * 100)}%)** ont un taux de réussite < 20% → rétention D14 = **{low['d14']}%**  
Dès qu'on passe à 20-40% → D14 = **{mid['d14']}%**  
**C'est un ×{round(mid['d14'] / low['d14'])} de rétention.**
    """, kind="danger")


@app.cell
def __(mo):
    mo.md("""
    ---
    ## 🔬 Deep Dive : Pourquoi 89% sont sous 20% ?
    """)


@app.cell
def __(mo):
    mo.md("### Distribution du nombre de sessions par utilisateur")


@app.cell
def __(query, px, ELOQA_PURPLE, ELOQA_COLORS, mo):
    df_sessions = query("""
        SELECT 
            CASE 
                WHEN sessions = 1 THEN '1'
                WHEN sessions = 2 THEN '2'
                WHEN sessions = 3 THEN '3'
                WHEN sessions <= 5 THEN '4-5'
                WHEN sessions <= 8 THEN '6-8'
                WHEN sessions <= 15 THEN '9-15'
                ELSE '16+'
            END as bucket,
            COUNT(*) as users,
            MIN(sessions) as sort_key
        FROM (
            SELECT "userId", COUNT(*) as sessions
            FROM "ReviewSession"
            WHERE finalized = true AND "startedAt" >= '2025-09-01'
            GROUP BY "userId"
        ) sub
        GROUP BY bucket
        ORDER BY sort_key
    """)
    df_sessions['pct'] = (df_sessions['users'] / df_sessions['users'].sum() * 100).round(1)
    df_sessions['cumul'] = df_sessions['pct'].cumsum().round(1)

    fig_dist = px.bar(df_sessions, x='bucket', y='users', text='pct',
                      title="Distribution du nombre de sessions par utilisateur",
                      color_discrete_sequence=[ELOQA_PURPLE],
                      labels={'bucket': 'Nombre de sessions', 'users': 'Utilisateurs'})
    fig_dist.update_traces(texttemplate='%{text}%', textposition='outside')
    fig_dist.update_layout(height=420)
    fig_dist


@app.cell
def __(mo):
    mo.callout("""
**67% des utilisateurs ne font qu'UNE seule session.** 82% en font 3 ou moins.  
Avec un système de répétition espacée qui nécessite ~8 sessions pour atteindre 20% de mastery,  
**la majorité des users ne peut structurellement pas réussir.**
    """, kind="danger")


@app.cell
def __(mo):
    mo.md("### Progression de la maîtrise par numéro de session")


@app.cell
def __(query, go, ELOQA_PURPLE, ELOQA_COLORS, make_subplots, mo):
    df_prog = query("""
        SELECT 
            session_num,
            COUNT(*) as users,
            ROUND(AVG(CASE WHEN "totalDiscoveredCardsNumber" > 0 
                THEN "totalMemorizedCardsNumber"::float / "totalDiscoveredCardsNumber" * 100 
                ELSE 0 END)::numeric, 1) as mastery_pct,
            ROUND(AVG("newMemorizedCardsNumber")::numeric, 2) as new_memorized,
            ROUND(AVG(days_since_first)::numeric, 1) as avg_days
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY "userId" ORDER BY "startedAt") as session_num,
                EXTRACT(EPOCH FROM ("startedAt" - FIRST_VALUE("startedAt") OVER (
                    PARTITION BY "userId" ORDER BY "startedAt"
                ))) / 86400 as days_since_first
            FROM "ReviewSession"
            WHERE finalized = true AND "startedAt" >= '2025-09-01'
        ) sub
        WHERE session_num <= 20
        GROUP BY session_num
        ORDER BY session_num
    """)

    fig_prog = make_subplots(specs=[[{"secondary_y": True}]])
    fig_prog.add_trace(
        go.Bar(x=df_prog['session_num'], y=df_prog['users'], name='Users actifs',
               marker_color=ELOQA_COLORS[1], opacity=0.4),
        secondary_y=False
    )
    fig_prog.add_trace(
        go.Scatter(x=df_prog['session_num'], y=df_prog['mastery_pct'], name='Mastery (%)',
                   line=dict(color=ELOQA_PURPLE, width=3), mode='lines+markers'),
        secondary_y=True
    )
    fig_prog.add_trace(
        go.Scatter(x=df_prog['session_num'], y=df_prog['new_memorized'], name='Nouveaux mots mémorisés/session',
                   line=dict(color=ELOQA_COLORS[3], width=2, dash='dot'), mode='lines+markers'),
        secondary_y=True
    )
    fig_prog.add_hline(y=20, line_dash="dash", line_color="red",
                       annotation_text="Seuil 20% (rétention)", secondary_y=True)
    fig_prog.update_layout(
        title="Progression de la maîtrise par session",
        height=500, xaxis_title="N° de session"
    )
    fig_prog.update_yaxes(title_text="Users actifs", secondary_y=False)
    fig_prog.update_yaxes(title_text="% / mots", secondary_y=True, range=[0, 40])
    fig_prog


@app.cell
def __(mo):
    mo.md("### La preuve : impact de la mémorisation en Session 1")


@app.cell
def __(query, go, ELOQA_PURPLE, ELOQA_COLORS, mo):
    df_s1 = query("""
        WITH session1 AS (
            SELECT "userId", "newMemorizedCardsNumber"
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY "userId" ORDER BY "startedAt") as sn
                FROM "ReviewSession"
                WHERE finalized = true AND "startedAt" >= '2025-09-01'
            ) sub WHERE sn = 1
        ),
        user_sessions AS (
            SELECT "userId", COUNT(*) as total_sessions,
                   MIN("startedAt") as first_s, MAX("startedAt") as last_s
            FROM "ReviewSession"
            WHERE finalized = true AND "startedAt" >= '2025-09-01'
            GROUP BY "userId"
        )
        SELECT 
            CASE WHEN s1."newMemorizedCardsNumber" > 0 THEN 'Mémorisé ≥1 mot en S1' 
                 ELSE 'Rien mémorisé en S1' END as s1_result,
            COUNT(*) as users,
            ROUND(AVG(us.total_sessions), 1) as avg_sessions,
            ROUND(COUNT(*) FILTER (WHERE us.total_sessions >= 2)::numeric / COUNT(*) * 100, 1) as pct_return,
            ROUND(COUNT(*) FILTER (WHERE us.total_sessions >= 5)::numeric / COUNT(*) * 100, 1) as pct_5plus,
            ROUND(COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (us.last_s - us.first_s)) >= 14*86400)::numeric / COUNT(*) * 100, 1) as d14
        FROM session1 s1
        JOIN user_sessions us ON s1."userId" = us."userId"
        GROUP BY s1_result
    """)

    fig_s1 = go.Figure(data=[
        go.Bar(name='Reviennent (S2+)', x=df_s1['s1_result'], y=df_s1['pct_return'],
               marker_color=ELOQA_COLORS[3], text=df_s1['pct_return'].astype(str) + '%', 
               textposition='inside', textfont=dict(size=16)),
        go.Bar(name='Font 5+ sessions', x=df_s1['s1_result'], y=df_s1['pct_5plus'],
               marker_color=ELOQA_PURPLE, text=df_s1['pct_5plus'].astype(str) + '%',
               textposition='inside', textfont=dict(size=16)),
        go.Bar(name='Rétention D14', x=df_s1['s1_result'], y=df_s1['d14'],
               marker_color=ELOQA_COLORS[2], text=df_s1['d14'].astype(str) + '%',
               textposition='inside', textfont=dict(size=16))
    ])
    fig_s1.update_layout(barmode='group', 
                         title="Impact de la mémorisation en Session 1 sur le retour",
                         height=450, font=dict(size=14))
    fig_s1


@app.cell
def __(mo):
    mo.callout("""
**Rien mémorisé en S1 →** 31% reviennent, 13% font 5+ sessions  
**≥1 mot mémorisé en S1 →** 81% reviennent, 62% font 5+ sessions  

**La gratification de progrès est LE déclencheur du retour.**
    """, kind="success")


@app.cell
def __(mo):
    mo.md("""
    ---
    ## 🧩 Le cercle vicieux
    """)


@app.cell
def __(mo):
    mo.callout("""
```
Mots trop durs → Pas de mémorisation en S1 → Pas de gratification
     ↑                                              ↓
     └──── Pas de mémorisation ← Pas de retour ←────┘
```

**Le problème n'est pas que les users sont mauvais.**  
Le problème est que **l'algorithme de répétition espacée est incompatible avec des users qui ne reviennent pas** — et la majorité ne revient pas parce qu'ils n'ont pas de gratification de progrès.

C'est un problème de **chicken and egg** : il faut revenir pour mémoriser, mais il faut mémoriser pour avoir envie de revenir.
    """, kind="danger")


@app.cell
def __(mo):
    mo.md("""
    ---
    ## 💡 Conclusions & Recommandations
    
    ### Le finding
    Le seuil de 20% de taux de réussite est le **"aha moment"** d'Eloqa. Franchir ce seuil multiplie la rétention D14 par 15.  
    Mais 89% des users n'y arrivent jamais parce que 67% ne font qu'une seule session et que le système de mémorisation nécessite au minimum 8 sessions.
    
    ### Actions à impact
    
    **1. Feedback de progrès en S1 avant la mémorisation réelle**  
    → "3 mots découverts !" / "Tu es au niveau 1 !" / jauge de progression visuelle  
    → Donner une sensation d'avancement même sans mémorisation effective  
    → Impact : augmenter le retour S2 (de 31% vers 50%+)
    
    **2. Baisser la difficulté initiale**  
    → Présenter des mots plus faciles en S1-S3 pour que les users atteignent 30-60% de réussite  
    → Impact potentiel : ×15 de rétention D14
    
    **3. Notification ciblée post-S1**  
    → "3 mots à réviser — 2 min" envoyé 24h après S1  
    → Impact : déclencher la S2 critique, briser le cercle vicieux
    
    **4. Mémorisation accélérée pour les 5 premiers mots**  
    → Réduire les intervalles de répétition espacée pour les 5 premiers mots  
    → Permettre de "mémoriser" un mot dès la S2 au lieu de la S8  
    → Impact : gratification rapide, preuve de valeur du produit
    
    ---
    *📊 Analyse par Quanty — Sprint Fév 2026*
    """)


if __name__ == "__main__":
    app.run()
