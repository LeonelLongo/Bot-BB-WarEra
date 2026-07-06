import os
import discord
import aiohttp
import asyncio
import json
import urllib.parse
import logging
from collections import deque
from dotenv import load_dotenv
from discord.ext import tasks

# --- Configuración de logging ---
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

load_dotenv()

# --- Validación de variables de entorno al arrancar ---
_required_vars = ["DISCORD_TOKEN", "CANAL_ID", "ROL_COMITE_ID", "API_KEY", "ID_ARGENTINA"]
for _var in _required_vars:
    if not os.getenv(_var):
        raise SystemExit(f"ERROR: Falta la variable de entorno '{_var}'. Revisá el archivo .env")

DISCORD_TOKEN   = os.getenv("DISCORD_TOKEN")
CANAL_ID        = int(os.getenv("CANAL_ID"))
ROL_COMITE_ID   = int(os.getenv("ROL_COMITE_ID"))
API_KEY         = os.getenv("API_KEY")
ID_ARGENTINA    = os.getenv("ID_ARGENTINA")

BASE_URL   = 'https://api2.warera.io/trpc'
STATE_FILE = 'estado.json'

def build_endpoint_usuarios():
    payload = {"countryId": ID_ARGENTINA, "limit": 50}
    return f"{BASE_URL}/user.getUsersByCountry?input={urllib.parse.quote(json.dumps(payload))}"

ENDPOINT_USUARIOS = build_endpoint_usuarios()

# --- Persistencia de estado entre reinicios ---
def cargar_estado() -> dict:
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def guardar_estado(ultima_fecha: str, conocidos: list):
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump({"ultima_fecha": ultima_fecha, "conocidos": conocidos}, f)
    except Exception as e:
        log.warning(f"No se pudo guardar el estado: {e}")

# --- Estado del bot encapsulado ---
class EstadoBot:
    def __init__(self):
        self.primera_carga        = True
        self.ultimo_nombre        = "Ninguno (esperando datos...)"
        self.ultima_fecha         = ""
        self.ultima_fecha_error   = None
        self.usuarios_conocidos   = deque(maxlen=5000)
        self._conocidos_set       = set()

        # Cargar estado persistido para no re-alertar en reinicios
        estado_guardado = cargar_estado()
        if estado_guardado:
            self.ultima_fecha = estado_guardado.get("ultima_fecha", "")
            for uid in estado_guardado.get("conocidos", []):
                self.usuarios_conocidos.append(uid)
                self._conocidos_set.add(uid)
            log.info(f"Estado cargado: {len(self._conocidos_set)} usuarios conocidos, última fecha: {self.ultima_fecha}")

    def agregar_usuario(self, user_id: str):
        if self.ya_conocido(user_id):
            return
        if len(self._conocidos_set) >= 5000:
            viejo = self.usuarios_conocidos.popleft()
            self._conocidos_set.discard(viejo)
        self.usuarios_conocidos.append(user_id)
        self._conocidos_set.add(user_id)

    def ya_conocido(self, user_id: str) -> bool:
        return user_id in self._conocidos_set

    def persistir(self):
        guardar_estado(self.ultima_fecha, list(self.usuarios_conocidos))

estado = EstadoBot()
cola_bienvenidas = asyncio.Queue()

# --- Session HTTP compartida (se inicializa en on_ready) ---
http_session: aiohttp.ClientSession = None

def get_headers() -> dict:
    return {'Authorization': f'Bearer {API_KEY}'}

# --- FUNCIÓN: Obtener nombre real (con 1 reintento) ---
async def obtener_nombre_usuario(user_id: str) -> str:
    input_encoded = urllib.parse.quote(json.dumps({"userId": user_id}))
    url = f"{BASE_URL}/user.getUserLite?input={input_encoded}"

    for intento in range(2):
        try:
            async with http_session.get(url, headers=get_headers(), timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    data = await r.json()
                    result_data = data.get('result', {}).get('data', {})
                    # Intenta ambas estructuras de respuesta
                    nombre = (
                        result_data.get('username')
                        or result_data.get('json', {}).get('username')
                    )
                    if nombre:
                        return nombre
        except asyncio.TimeoutError:
            log.warning(f"Timeout obteniendo nombre de {user_id} (intento {intento+1})")
        except Exception as e:
            log.warning(f"Error obteniendo nombre de {user_id}: {e}")

        if intento == 0:
            await asyncio.sleep(2)  # Pausa antes del reintento

    return f"Ciudadano_{user_id[-6:]}"

# --- TAREA: Reporte de estado cada 10 minutos ---
@tasks.loop(minutes=10)
async def reporte_estado():
    canal = client.get_channel(CANAL_ID)
    if not canal:
        return

    hay_error = estado.ultima_fecha_error is not None
    embed = discord.Embed(
        title="🟢 Reporte del Escáner" if not hay_error else "🟡 Reporte del Escáner (con advertencias)",
        description="El sistema de Alianza Libertaria sigue en línea y monitoreando las fronteras del país.",
        color=discord.Color.green() if not hay_error else discord.Color.yellow()
    )
    embed.add_field(
        name="Último ciudadano detectado",
        value=f"**{estado.ultimo_nombre}**",
        inline=False
    )
    if hay_error:
        embed.add_field(
            name="⚠️ Último error registrado",
            value=f"`{estado.ultima_fecha_error}`",
            inline=False
        )
    embed.set_footer(text="Ministerio de Economía • Sistema Automático")
    await canal.send(embed=embed)

@reporte_estado.before_loop
async def antes_de_reportar():
    await client.wait_until_ready()
    while estado.primera_carga:
        await asyncio.sleep(1)

# --- TAREA: Despachador de cola (anti-spam babyboom) ---
@tasks.loop(seconds=2)
async def despachador_mensajes():
    if cola_bienvenidas.empty():
        return
    nombre, link = await cola_bienvenidas.get()
    try:
        await enviar_alerta_discord(nombre, link)
    except discord.HTTPException as e:
        if e.status == 429:
            # Rate limit: reencola y espera
            await cola_bienvenidas.put((nombre, link))
            log.warning(f"Rate limit alcanzado, reencolando a {nombre}. Retry-After: {e.retry_after:.1f}s")
            await asyncio.sleep(e.retry_after + 0.5)
        else:
            log.error(f"Error HTTP enviando alerta para {nombre}: {e}")
    except Exception as e:
        log.error(f"Error inesperado en despachador: {e}")
    finally:
        cola_bienvenidas.task_done()

@despachador_mensajes.before_loop
async def antes_de_despachar():
    await client.wait_until_ready()

# --- TAREA: Escáner principal ---
@tasks.loop(seconds=20)
async def consultar_nuevos_usuarios():
    try:
        async with http_session.get(
            ENDPOINT_USUARIOS,
            headers=get_headers(),
            timeout=aiohttp.ClientTimeout(total=15)
        ) as respuesta:
            if respuesta.status != 200:
                log.warning(f"API respondió {respuesta.status}")
                return

            data = await respuesta.json()
            items = data.get('result', {}).get('data', {}).get('items', [])

            if not items:
                log.warning("La API devolvió lista vacía.")
                return

            # Ordenar de más nuevo a más viejo
            items.sort(key=lambda x: x.get('createdAt', ''), reverse=True)

            if estado.primera_carga:
                estado.ultima_fecha = items[0].get('createdAt', '')
                for u in items:
                    estado.agregar_usuario(u.get('_id'))
                nombre = await obtener_nombre_usuario(items[0].get('_id'))
                estado.ultimo_nombre = nombre
                estado.ultima_fecha_error = None
                estado.persistir()
                log.info(f"Escaneo inicial listo. Último ciudadano: {nombre}")
                estado.primera_carga = False

            else:
                # Iterar de más viejo a más nuevo para mantener orden cronológico en la cola
                for usuario in reversed(items):
                    user_id      = usuario.get('_id')
                    fecha_creado = usuario.get('createdAt', '')

                    if not estado.ya_conocido(user_id) and fecha_creado > estado.ultima_fecha:
                        estado.agregar_usuario(user_id)
                        estado.ultima_fecha = fecha_creado
                        estado.persistir()

                        nombre = await obtener_nombre_usuario(user_id)
                        estado.ultimo_nombre = nombre
                        
                        # Corrección: URL limpia sin formato Markdown
                        link   = f"https://warera.io/profile/{user_id}"

                        await cola_bienvenidas.put((nombre, link))
                        log.info(f"[NUEVO] {nombre} añadido a la cola.")

    except asyncio.TimeoutError:
        msg = "Timeout al consultar la API"
        log.error(msg)
        estado.ultima_fecha_error = msg
    except Exception as e:
        msg = str(e)
        log.error(f"Error en el escáner: {msg}")
        estado.ultima_fecha_error = msg

@consultar_nuevos_usuarios.before_loop
async def antes_de_escanear():
    await client.wait_until_ready()

# --- Enviar alerta a Discord ---
async def enviar_alerta_discord(nombre: str, link: str):
    canal = client.get_channel(CANAL_ID)
    if not canal:
        log.warning("Canal no encontrado.")
        return

    embed = discord.Embed(
        title="🇦🇷 ¡Nuevo Ciudadano Detectado!",
        description=(
            f"El jugador **{nombre}** acaba de registrarse en Argentina.\n\n"
            "¡Vamos a invitarlo a Alianza Libertaria antes de que se lo lleven!"
        ),
        color=discord.Color.blue(),
        url=link
    )
    embed.set_footer(text="Ministerio de Economía • Sistema de Rastreo Automático")

    view = discord.ui.View()
    view.add_item(discord.ui.Button(
        label='Ver Perfil en WarEra',
        url=link,
        style=discord.ButtonStyle.link
    ))

    await canal.send(content=f"<@&{ROL_COMITE_ID}>", embed=embed, view=view)

# --- Eventos del cliente ---
intents = discord.Intents.default()
client  = discord.Client(intents=intents)

@client.event
async def on_ready():
    global http_session
    if http_session is None or http_session.closed:
        http_session = aiohttp.ClientSession()
    log.info(f'Bot conectado como {client.user}')
    despachador_mensajes.start()
    consultar_nuevos_usuarios.start()
    reporte_estado.start()

@client.event
async def on_close():
    if http_session and not http_session.closed:
        await http_session.close()
        log.info("Sesión HTTP cerrada correctamente.")

if __name__ == "__main__":
    client.run(DISCORD_TOKEN)