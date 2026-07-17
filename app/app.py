"""Simulador didático de telemetria pluviométrica via MQTT local.

O processo representado é: pluviômetro simulado -> payload JSON -> broker MQTT
local -> visualização na interface. Ele não simula nem valida rádio LoRa,
Meshtastic ou a entrega de uma mensagem a um dispositivo móvel real.
"""

from __future__ import annotations

import json
import os
import threading
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import gradio as gr
import paho.mqtt.client as mqtt


def read_int(name: str, default: int) -> int:
    """Lê uma variável inteira sem tornar a inicialização frágil."""
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def read_float(name: str, default: float) -> float:
    """Lê uma variável decimal sem tornar a inicialização frágil."""
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    mqtt_host: str
    mqtt_port: int
    mqtt_topic: str
    mqtt_client_id: str
    mqtt_username: str
    mqtt_password: str
    mqtt_qos: int
    tip_mm: float
    sample_interval_minutes: int
    accumulation_window_minutes: int
    gradio_port: int


def load_settings() -> Settings:
    return Settings(
        mqtt_host=os.getenv("MQTT_HOST", "mqtt"),
        mqtt_port=read_int("MQTT_PORT", 1883),
        mqtt_topic=os.getenv("MQTT_TOPIC", "americas-techguard/simulation/rainfall"),
        mqtt_client_id=os.getenv("MQTT_CLIENT_ID", "techguard-gradio-simulator"),
        mqtt_username=os.getenv("MQTT_USERNAME", ""),
        mqtt_password=os.getenv("MQTT_PASSWORD", ""),
        mqtt_qos=max(0, min(read_int("MQTT_QOS", 1), 2)),
        tip_mm=read_float("RAIN_GAUGE_TIP_MM", 0.2),
        sample_interval_minutes=max(1, read_int("SAMPLE_INTERVAL_MINUTES", 5)),
        accumulation_window_minutes=max(1, read_int("ACCUMULATION_WINDOW_MINUTES", 60)),
        gradio_port=read_int("GRADIO_SERVER_PORT", 7860),
    )


SETTINGS = load_settings()

# Limiares exclusivamente didáticos: requerem calibração meteorológica local
# antes de qualquer uso operacional.
RISK_THRESHOLDS_MM_1H = {
    "safe_upper": 5.0,
    "attention_upper": 20.0,
    "alert_upper": 40.0,
}


def classify_risk(accumulated_mm_1h: float) -> str:
    if accumulated_mm_1h < RISK_THRESHOLDS_MM_1H["safe_upper"]:
        return "safe"
    if accumulated_mm_1h < RISK_THRESHOLDS_MM_1H["attention_upper"]:
        return "attention"
    if accumulated_mm_1h < RISK_THRESHOLDS_MM_1H["alert_upper"]:
        return "alert"
    return "critical"


def alert_message(risk_level: str) -> str:
    messages = {
        "safe": "Monitoramento normal.",
        "attention": "Atenção: acumulado de chuva acima do limiar didático.",
        "alert": "Alerta: avaliar as condições locais.",
        "critical": "Crítico: acionar o protocolo local previamente definido.",
    }
    return messages[risk_level]


class RainfallSimulator:
    """Converte chuva informada em leituras de uma caçamba basculante simulada."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._history: deque[tuple[datetime, float]] = deque()
        self._lock = threading.Lock()

    def build_payload(
        self,
        rainfall_mm: float,
        device_id: str,
    ) -> tuple[dict[str, Any], tuple[datetime, float]]:
        if rainfall_mm < 0:
            raise ValueError("A chuva do intervalo não pode ser negativa.")
        if not device_id.strip():
            raise ValueError("Informe um identificador para o dispositivo simulado.")
        if self.settings.tip_mm <= 0:
            raise ValueError("RAIN_GAUGE_TIP_MM deve ser maior que zero.")

        timestamp = datetime.now(timezone.utc)
        tip_count = int(rainfall_mm / self.settings.tip_mm + 0.5)
        measured_mm = round(tip_count * self.settings.tip_mm, 3)

        with self._lock:
            cutoff = timestamp - timedelta(minutes=self.settings.accumulation_window_minutes)
            while self._history and self._history[0][0] < cutoff:
                self._history.popleft()
            accumulated_mm = round(sum(value for _, value in self._history) + measured_mm, 3)

        intensity_mm_h = round(measured_mm * 60 / self.settings.sample_interval_minutes, 3)
        risk_level = classify_risk(accumulated_mm)
        payload = {
            "schema_version": "1.0",
            "reading_id": f"rain-{timestamp.strftime('%Y%m%dT%H%M%S%fZ')}-{uuid.uuid4().hex[:6]}",
            "device_id": device_id.strip(),
            "timestamp": timestamp.isoformat(),
            "sensor_type": "rainfall",
            "sensor_value": measured_mm,
            "unit": "mm",
            "source": "simulation_gradio_mqtt",
            "sensor_simulation": {
                "status": "ok",
                "tip_count": tip_count,
                "tip_mm": self.settings.tip_mm,
                "rainfall_mm": measured_mm,
            },
            "rainfall_intensity_mm_per_h": intensity_mm_h,
            "rainfall_accumulated_mm_1h": accumulated_mm,
            "risk_level": risk_level,
            "alert_message": alert_message(risk_level),
        }
        return payload, (timestamp, measured_mm)

    def commit(self, entry: tuple[datetime, float]) -> None:
        """Inclui a leitura no acumulado somente depois da publicação confirmada."""
        with self._lock:
            self._history.append(entry)

    def reset(self) -> None:
        with self._lock:
            self._history.clear()


class MqttBridge:
    """Publica e acompanha mensagens de um único tópico MQTT local."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._connected = False
        self._detail = "Aguardando conexão com o broker MQTT."
        self._messages: deque[dict[str, Any]] = deque(maxlen=20)
        self._lock = threading.Lock()
        self._message_received = threading.Event()
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=settings.mqtt_client_id,
            protocol=mqtt.MQTTv311,
        )
        if settings.mqtt_username:
            self.client.username_pw_set(settings.mqtt_username, settings.mqtt_password)
        self.client.reconnect_delay_set(min_delay=1, max_delay=10)
        self.client.on_connect = self._on_connect
        self.client.on_connect_fail = self._on_connect_fail
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

    def start(self) -> None:
        self.client.connect_async(self.settings.mqtt_host, self.settings.mqtt_port, keepalive=30)
        self.client.loop_start()

    def _on_connect(
        self,
        client: mqtt.Client,
        _userdata: Any,
        _flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        _properties: mqtt.Properties | None,
    ) -> None:
        if reason_code == 0:
            result, _ = client.subscribe(self.settings.mqtt_topic, qos=self.settings.mqtt_qos)
            with self._lock:
                self._connected = result == mqtt.MQTT_ERR_SUCCESS
                self._detail = (
                    f"Conectado e inscrito em `{self.settings.mqtt_topic}`."
                    if self._connected
                    else f"Conectado, mas não foi possível assinar o tópico (código {result})."
                )
        else:
            with self._lock:
                self._connected = False
                self._detail = f"Broker recusou a conexão: {reason_code}."

    def _on_connect_fail(self, _client: mqtt.Client, _userdata: Any) -> None:
        with self._lock:
            self._connected = False
            self._detail = "Não foi possível conectar ao broker; nova tentativa automática em andamento."

    def _on_disconnect(
        self,
        _client: mqtt.Client,
        _userdata: Any,
        _disconnect_flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        _properties: mqtt.Properties | None,
    ) -> None:
        with self._lock:
            self._connected = False
            self._detail = f"Desconectado do broker ({reason_code}); aguardando reconexão."

    def _on_message(self, _client: mqtt.Client, _userdata: Any, message: mqtt.MQTTMessage) -> None:
        try:
            payload: Any = json.loads(message.payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            payload = {"raw_payload": message.payload.decode("utf-8", errors="replace")}
        received = {
            "received_at": datetime.now(timezone.utc).isoformat(),
            "topic": message.topic,
            "qos": message.qos,
            "payload": payload,
        }
        with self._lock:
            self._messages.appendleft(received)
        self._message_received.set()

    def publish(self, payload: dict[str, Any]) -> tuple[bool, str]:
        with self._lock:
            connected = self._connected
        if not connected:
            return False, "Broker indisponível: aguarde a conexão MQTT antes de publicar."

        message = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        self._message_received.clear()
        info = self.client.publish(self.settings.mqtt_topic, message, qos=self.settings.mqtt_qos)
        info.wait_for_publish(timeout=3)
        if info.rc != mqtt.MQTT_ERR_SUCCESS or not info.is_published():
            return False, f"Publicação não confirmada pelo broker (código {info.rc})."
        # Aguarda brevemente o eco da própria assinatura para atualizar a interface.
        self._message_received.wait(timeout=1)
        return True, f"Publicação confirmada no tópico `{self.settings.mqtt_topic}` ({len(message.encode('utf-8'))} bytes)."

    def status(self) -> str:
        with self._lock:
            status = "conectado" if self._connected else "desconectado"
            return f"**MQTT: {status}.** {self._detail}"

    def received_messages(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._messages)

    def clear_messages(self) -> None:
        with self._lock:
            self._messages.clear()


SIMULATOR = RainfallSimulator(SETTINGS)
BRIDGE = MqttBridge(SETTINGS)


def publish_simulation(rainfall_mm: float, device_id: str) -> tuple[dict[str, Any], str, list[dict[str, Any]]]:
    try:
        payload, history_entry = SIMULATOR.build_payload(float(rainfall_mm), device_id)
    except (TypeError, ValueError) as error:
        return {}, f"**Entrada inválida:** {error}", BRIDGE.received_messages()

    published, detail = BRIDGE.publish(payload)
    if published:
        SIMULATOR.commit(history_entry)
        status = f"**Leitura enviada.** {detail}"
    else:
        status = f"**Leitura não enviada.** {detail}"
    return payload, f"{status}\n\n{BRIDGE.status()}", BRIDGE.received_messages()


def refresh_dashboard() -> tuple[str, list[dict[str, Any]]]:
    return BRIDGE.status(), BRIDGE.received_messages()


def clear_dashboard() -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
    SIMULATOR.reset()
    BRIDGE.clear_messages()
    return f"**Histórico limpo.** {BRIDGE.status()}", [], {}


with gr.Blocks(title="Simulador de chuva MQTT") as demo:
    gr.Markdown(
        "# Simulador pluviométrico: JSON → MQTT → visualização\n"
        "A interface representa um pluviômetro de caçamba basculante simulado. "
        "O broker é local e valida somente a troca MQTT sobre IP; ela não substitui "
        "testes LoRa, Meshtastic, RF, alcance ou dispositivos móveis reais."
    )
    with gr.Row():
        rainfall_input = gr.Slider(
            minimum=0,
            maximum=30,
            value=5,
            step=0.2,
            label="Chuva no intervalo simulado (mm)",
        )
        device_input = gr.Textbox(value="sim-rain-node-01", label="Identificador do dispositivo")
    with gr.Row():
        publish_button = gr.Button("Simular e publicar", variant="primary")
        refresh_button = gr.Button("Atualizar visualização")
        clear_button = gr.Button("Limpar histórico")

    mqtt_status = gr.Markdown(BRIDGE.status())
    with gr.Row():
        payload_view = gr.JSON(label="Payload JSON publicado", value={})
        received_view = gr.JSON(label="Mensagens recebidas no tópico MQTT", value=[])
    gr.Markdown(
        "Os níveis de risco são limiares didáticos. Para uso operacional, eles exigem "
        "calibração com dados locais e validação do protocolo de alerta."
    )

    publish_button.click(
        publish_simulation,
        inputs=[rainfall_input, device_input],
        outputs=[payload_view, mqtt_status, received_view],
    )
    refresh_button.click(refresh_dashboard, outputs=[mqtt_status, received_view])
    clear_button.click(clear_dashboard, outputs=[mqtt_status, received_view, payload_view])


if __name__ == "__main__":
    # O Docker expõe o serviço somente em loopback no host via docker-compose.yml.
    BRIDGE.start()
    demo.launch(server_name="0.0.0.0", server_port=SETTINGS.gradio_port, show_error=True)
