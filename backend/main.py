"""
Dominicana Sports Live - Backend
---------------------------------
Consulta la API pública y gratuita de MLB (statsapi.mlb.com) en tiempo real,
filtra jugadas de jugadores dominicanos, y las empuja instantaneamente
a todos los clientes conectados via WebSocket.

Diseño pensado para velocidad:
- asyncio + aiohttp: peticiones HTTP no bloqueantes, muchas en paralelo
- Polling diferenciado: juegos en vivo se consultan cada POLL_LIVE_SECONDS,
  el calendario general solo cada POLL_SCHEDULE_SECONDS (no hay necesidad
  de golpear la API por juegos que no han empezado)
- Set de "jugadas ya vistas" por juego para nunca reenviar duplicados
- Broadcast en memoria a todos los WebSockets conectados (sin DB de por medio)

Correr con:  uvicorn main:app --reload --port 8000
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import aiohttp
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dr-sports-live")

# ---------- Configuración ----------
POLL_LIVE_SECONDS = 6
POLL_SCHEDULE_SECONDS = 120
RECENT_CACHE_SECONDS = 300
MLB_BASE = "https://statsapi.mlb.com/api"

PLAYERS_FILE = Path(__file__).parent / "players_do.json"
IDS_CACHE_FILE = Path(__file__).parent / "player_ids_cache.json"


def cargar_jugadores_dominicanos() -> set[str]:
    data = json.loads(PLAYERS_FILE.read_text(encoding="utf-8"))
    return {j["full_name"] for j in data["jugadores"]}


DOMINICANOS = cargar_jugadores_dominicanos()
log.info(f"Cargados {len(DOMINICANOS)} jugadores dominicanos en el filtro")

NAME_TO_ID: dict[str, int] = {}
NAME_TO_PAIS: dict[str, str] = {}

BANDERAS = {
    "Dominican Republic": "🇩🇴", "USA": "🇺🇸", "United States": "🇺🇸",
    "Venezuela": "🇻🇪", "Puerto Rico": "🇵🇷", "Cuba": "🇨🇺", "Mexico": "🇲🇽",
    "Japan": "🇯🇵", "Colombia": "🇨🇴", "Panama": "🇵🇦", "Curacao": "🇨🇼",
    "Netherlands": "🇳🇱", "South Korea": "🇰🇷", "Canada": "🇨🇦", "Aruba": "🇦🇼",
    "Nicaragua": "🇳🇮", "Australia": "🇦🇺", "Bahamas": "🇧🇸", "Brazil": "🇧🇷",
    "Taiwan": "🇹🇼", "Honduras": "🇭🇳", "Germany": "🇩🇪",
}


def foto_url(nombre: str):
    pid = NAME_TO_ID.get(nombre)
    if not pid:
        return None
    return f"https://img.mlbstatic.com/mlb-photos/image/upload/w_180,q_100/v1/people/{pid}/headshot/67/current.png"


def pais_y_bandera(nombre: str):
    pais = NAME_TO_PAIS.get(nombre, "")
    return pais, BANDERAS.get(pais, "")


async def resolver_ids_jugadores(session: aiohttp.ClientSession):
    global NAME_TO_ID, NAME_TO_PAIS
    if IDS_CACHE_FILE.exists():
        try:
            cache = json.loads(IDS_CACHE_FILE.read_text(encoding="utf-8"))
            hoy = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if cache.get("fecha") == hoy:
                NAME_TO_ID = dict(cache.get("ids", {}))
                NAME_TO_PAIS = dict(cache.get("paises", {}))
                log.info(f"[directorio] {len(NAME_TO_ID)} ids y {len(NAME_TO_PAIS)} paises cargados desde cache local")
                return
        except Exception:
            pass

    anio = datetime.now(timezone.utc).year
    url = f"{MLB_BASE}/v1/sports/1/players?season={anio}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            data = await resp.json()
        ids_encontrados = {}
        paises_encontrados = {}
        for persona in data.get("people", []):
            nombre = persona.get("fullName", "")
            if not nombre:
                continue
            paises_encontrados[nombre] = persona.get("birthCountry", "")
            if nombre in DOMINICANOS:
                ids_encontrados[nombre] = persona.get("id")
        NAME_TO_ID = ids_encontrados
        NAME_TO_PAIS = paises_encontrados
        IDS_CACHE_FILE.write_text(
            json.dumps({"fecha": datetime.now(timezone.utc).strftime("%Y-%m-%d"), "ids": ids_encontrados, "paises": paises_encontrados}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log.info(f"[directorio] {len(ids_encontrados)}/{len(DOMINICANOS)} fotos y {len(paises_encontrados)} paises resueltos desde la API")
    except Exception as e:
        log.warning(f"[directorio] no se pudo resolver el directorio de jugadores: {e}")


# ---------- Manejo de conexiones WebSocket ----------
class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
        log.info(f"Cliente conectado. Total: {len(self.active)}")

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)
        log.info(f"Cliente desconectado. Total: {len(self.active)}")

    async def broadcast(self, message: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()

# Estado en memoria: qué jugadas (por gamePk) ya se enviaron, para no duplicar
juegos_vistos: dict[int, set[str]] = {}
# Cache simple de resultados ya cerrados para no re-anunciar el cierre
juegos_cerrados_anunciados: set[int] = set()


# ---------- Lógica de polling ----------
async def obtener_juegos_del_dia(session: aiohttp.ClientSession) -> list[dict]:
    hoy = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = f"{MLB_BASE}/v1/schedule?sportId=1&date={hoy}&hydrate=team"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        data = await resp.json()
    juegos = []
    for fecha in data.get("dates", []):
        juegos.extend(fecha.get("games", []))
    return juegos


SEMAFORO_MLB = asyncio.Semaphore(8)


async def obtener_feed_en_vivo(session: aiohttp.ClientSession, game_pk: int) -> Optional[dict]:
    url = f"{MLB_BASE}/v1.1/game/{game_pk}/feed/live"
    async with SEMAFORO_MLB:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                return await resp.json()
        except Exception as e:
            log.warning(f"Error consultando feed de juego {game_pk}: {e}")
            return None


def extraer_jugadas_dominicanas(feed: dict, game_pk: int) -> list[dict]:
    """Recorre las jugadas (allPlays) del feed en vivo y devuelve solo
    las que involucran a un jugador dominicano y aún no fueron enviadas."""
    resultado = []
    vistos = juegos_vistos.setdefault(game_pk, set())

    live_data = feed.get("liveData", {})
    plays = live_data.get("plays", {}).get("allPlays", [])
    game_data = feed.get("gameData", {})
    home = game_data.get("teams", {}).get("home", {}).get("name", "")
    away = game_data.get("teams", {}).get("away", {}).get("name", "")

    for play in plays:
        about = play.get("about", {})
        play_id = about.get("atBatIndex")
        clave = f"{game_pk}-{play_id}"
        if clave in vistos:
            continue
        if not about.get("isComplete", False):
            continue  # solo jugadas ya terminadas, para no mandar info parcial

        matchup = play.get("matchup", {})
        bateador = matchup.get("batter", {}).get("fullName", "")
        pitcher = matchup.get("pitcher", {}).get("fullName", "")

        involucrado = None
        rol = None
        if bateador in DOMINICANOS:
            involucrado = bateador
            rol = "bateador"
        elif pitcher in DOMINICANOS:
            involucrado = pitcher
            rol = "pitcher"

        if involucrado:
            result = play.get("result", {})
            evento = result.get("event", "")
            descripcion = result.get("description", "")

            vistos.add(clave)
            resultado.append({
                "type": "jugada",
                "game_pk": game_pk,
                "matchup": f"{away} @ {home}",
                "jugador": involucrado,
                "rol": rol,
                "evento": evento,
                "descripcion": descripcion,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
        else:
            # igual lo marcamos como visto para no reprocesarlo cada polling
            vistos.add(clave)

    return resultado


def lineas_dominicanos_de_boxscore(feed: dict) -> list[dict]:
    box = feed.get("liveData", {}).get("boxscore", {})
    lineas = []
    for lado in ("home", "away"):
        equipo = box.get("teams", {}).get(lado, {})
        players = equipo.get("players", {})
        for _, pdata in players.items():
            nombre = pdata.get("person", {}).get("fullName", "")
            if nombre not in DOMINICANOS:
                continue
            stats = pdata.get("stats", {}).get("batting", {})
            pstats = pdata.get("stats", {}).get("pitching", {})
            linea = {"jugador": nombre, "foto": foto_url(nombre)}
            if stats and stats.get("atBats", 0) > 0:
                linea["tipo"] = "bateo"
                linea["texto"] = f"{stats.get('hits', 0)}-{stats.get('atBats', 0)}, {stats.get('homeRuns', 0)} HR, {stats.get('rbi', 0)} CI"
                lineas.append(linea)
            elif pstats and pstats.get("inningsPitched") and pstats.get("inningsPitched") != "0.0":
                linea["tipo"] = "pitcheo"
                linea["texto"] = f"{pstats.get('inningsPitched')} IP, {pstats.get('strikeOuts', 0)} K, {pstats.get('earnedRuns', 0)} CL"
                lineas.append(linea)
    return lineas


def resumen_final_dominicanos(feed: dict, game_pk: int) -> Optional[dict]:
    game_data = feed.get("gameData", {})
    status = game_data.get("status", {}).get("abstractGameState", "")
    if status != "Final" or game_pk in juegos_cerrados_anunciados:
        return None

    lineas = lineas_dominicanos_de_boxscore(feed)
    if not lineas:
        return None

    home = game_data.get("teams", {}).get("home", {}).get("name", "")
    away = game_data.get("teams", {}).get("away", {}).get("name", "")
    linescore = feed.get("liveData", {}).get("linescore", {})
    home_runs = linescore.get("teams", {}).get("home", {}).get("runs")
    away_runs = linescore.get("teams", {}).get("away", {}).get("runs")

    juegos_cerrados_anunciados.add(game_pk)
    return {
        "type": "resumen_final",
        "game_pk": game_pk,
        "matchup": f"{away} {away_runs} - {home_runs} {home}",
        "lineas_dominicanos": [l["jugador"] + ": " + l["texto"] for l in lineas],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def obtener_juegos_por_rango(session: aiohttp.ClientSession, dias_atras: int, dias_adelante: int = 0) -> list[dict]:
    hoy = datetime.now(timezone.utc)
    inicio = (hoy - timedelta(days=dias_atras)).strftime("%Y-%m-%d")
    fin = (hoy + timedelta(days=dias_adelante)).strftime("%Y-%m-%d")
    url = f"{MLB_BASE}/v1/schedule?sportId=1&startDate={inicio}&endDate={fin}&hydrate=team"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        data = await resp.json()
    juegos = []
    for fecha in data.get("dates", []):
        juegos.extend(fecha.get("games", []))
    return juegos


_cache_recent: dict = {"timestamp": 0, "data": None}


async def construir_recientes(dias: int = 2) -> list[dict]:
    async with aiohttp.ClientSession() as session:
        juegos = await obtener_juegos_por_rango(session, dias_atras=dias)
        finales = [g for g in juegos if g.get("status", {}).get("abstractGameState") == "Final"]
        tareas = [obtener_feed_en_vivo(session, g["gamePk"]) for g in finales]
        feeds = await asyncio.gather(*tareas)

    resultado = []
    for g, feed in zip(finales, feeds):
        if not feed:
            continue
        lineas = lineas_dominicanos_de_boxscore(feed)
        if not lineas:
            continue

        game_data = feed.get("gameData", {})
        home = game_data.get("teams", {}).get("home", {}).get("name", "")
        away = game_data.get("teams", {}).get("away", {}).get("name", "")
        linescore = feed.get("liveData", {}).get("linescore", {})
        home_runs = linescore.get("teams", {}).get("home", {}).get("runs")
        away_runs = linescore.get("teams", {}).get("away", {}).get("runs")
        fecha_juego = g.get("officialDate", "")

        resultado.append({
            "game_pk": g["gamePk"],
            "fecha": fecha_juego,
            "matchup": f"{away} @ {home}",
            "marcador": f"{away_runs} - {home_runs}",
            "lineas_dominicanos": lineas,
        })

    resultado.sort(key=lambda x: x["fecha"], reverse=True)
    return resultado


def armar_estado_juego(feed: dict, game_pk: int) -> dict:
    game_data = feed.get("gameData", {})
    linescore = feed.get("liveData", {}).get("linescore", {})
    home = game_data.get("teams", {}).get("home", {}).get("name", "")
    away = game_data.get("teams", {}).get("away", {}).get("name", "")
    home_runs = linescore.get("teams", {}).get("home", {}).get("runs", 0)
    away_runs = linescore.get("teams", {}).get("away", {}).get("runs", 0)
    offense = linescore.get("offense", {})
    defense = linescore.get("defense", {})
    bateador = offense.get("batter", {}).get("fullName", "")
    pitcher = defense.get("pitcher", {}).get("fullName", "")

    bateador_pais, bateador_bandera = pais_y_bandera(bateador)
    pitcher_pais, pitcher_bandera = pais_y_bandera(pitcher)

    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    ultima_jugada = None
    for jugada in reversed(plays):
        if jugada.get("about", {}).get("isComplete"):
            result = jugada.get("result", {})
            bateador_jugada = jugada.get("matchup", {}).get("batter", {}).get("fullName", "")
            ultima_jugada = {
                "evento": result.get("event", ""),
                "descripcion": result.get("description", ""),
                "jugador": bateador_jugada,
                "es_dominicano": bateador_jugada in DOMINICANOS,
            }
            break

    return {
        "game_pk": game_pk,
        "matchup": f"{away} @ {home}",
        "marcador_visitante": away_runs,
        "marcador_local": home_runs,
        "entrada": linescore.get("currentInning"),
        "entrada_estado": linescore.get("inningState", ""),
        "outs": linescore.get("outs", 0),
        "bolas": linescore.get("balls", 0),
        "strikes": linescore.get("strikes", 0),
        "bases": {
            "primera": bool(offense.get("first")),
            "segunda": bool(offense.get("second")),
            "tercera": bool(offense.get("third")),
        },
        "bateador": bateador,
        "bateador_es_dominicano": bateador in DOMINICANOS,
        "bateador_pais": bateador_pais,
        "bateador_bandera": bateador_bandera,
        "pitcher": pitcher,
        "pitcher_es_dominicano": pitcher in DOMINICANOS,
        "pitcher_pais": pitcher_pais,
        "pitcher_bandera": pitcher_bandera,
        "ultima_jugada": ultima_jugada,
        "estado_detallado": game_data.get("status", {}).get("detailedState", ""),
    }


estado_vivo: dict[int, dict] = {}


async def loop_polling():
    global estado_vivo
    async with aiohttp.ClientSession() as session:
        ultimo_refresh_calendario = 0
        juegos_del_dia: list[dict] = []

        while True:
            ahora = asyncio.get_event_loop().time()

            if ahora - ultimo_refresh_calendario > POLL_SCHEDULE_SECONDS or not juegos_del_dia:
                try:
                    juegos_del_dia = await obtener_juegos_del_dia(session)
                    ultimo_refresh_calendario = ahora
                    log.info(f"Calendario actualizado: {len(juegos_del_dia)} juegos hoy")
                except Exception as e:
                    log.error(f"Error obteniendo calendario: {e}")

            en_vivo = [
                g for g in juegos_del_dia
                if g.get("status", {}).get("abstractGameState") == "Live"
            ]

            if en_vivo:
                tareas = [obtener_feed_en_vivo(session, g["gamePk"]) for g in en_vivo]
                feeds = await asyncio.gather(*tareas)

                nuevo_estado_vivo = {}
                for g, feed in zip(en_vivo, feeds):
                    if not feed:
                        continue
                    game_pk = g["gamePk"]
                    nuevo_estado_vivo[game_pk] = armar_estado_juego(feed, game_pk)

                    for jugada in extraer_jugadas_dominicanas(feed, game_pk):
                        log.info(f"JUGADA DOMINICANA: {jugada['jugador']} - {jugada['evento']}")
                        await manager.broadcast(jugada)

                    resumen = resumen_final_dominicanos(feed, game_pk)
                    if resumen:
                        log.info(f"RESUMEN FINAL: {resumen['matchup']}")
                        await manager.broadcast(resumen)

                estado_vivo = nuevo_estado_vivo
            else:
                estado_vivo = {}

            await asyncio.sleep(POLL_LIVE_SECONDS)


# ---------- App FastAPI ----------
app = FastAPI(title="Dominicana Sports Live")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    async with aiohttp.ClientSession() as session:
        await resolver_ids_jugadores(session)
    asyncio.create_task(loop_polling())
    log.info("Loop de polling iniciado")


@app.get("/health")
async def health():
    return {"status": "ok", "jugadores_en_filtro": len(DOMINICANOS)}


@app.get("/api/today")
async def api_today():
    async with aiohttp.ClientSession() as session:
        juegos = await obtener_juegos_del_dia(session)

    resultado = []
    for g in juegos:
        teams = g.get("teams", {})
        resultado.append({
            "game_pk": g.get("gamePk"),
            "matchup": f"{teams.get('away', {}).get('team', {}).get('name', '')} @ {teams.get('home', {}).get('team', {}).get('name', '')}",
            "hora_utc": g.get("gameDate"),
            "estado": g.get("status", {}).get("detailedState", ""),
        })
    return {"fecha": datetime.now(timezone.utc).strftime("%Y-%m-%d"), "juegos": resultado}


@app.get("/api/recent")
async def api_recent(dias: int = 2):
    ahora = asyncio.get_event_loop().time()
    if _cache_recent["data"] is not None and (ahora - _cache_recent["timestamp"] < RECENT_CACHE_SECONDS):
        return {"juegos": _cache_recent["data"], "cache": True}

    datos = await construir_recientes(dias=dias)
    _cache_recent["data"] = datos
    _cache_recent["timestamp"] = ahora
    return {"juegos": datos, "cache": False}


@app.get("/api/live")
async def api_live():
    return {"juegos": list(estado_vivo.values())}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


FRONTEND_DIR = Path(__file__).parent.parent / "frontend"
app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
