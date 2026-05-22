import streamlit as st
import requests
import time
import plotly.graph_objects as go

API_URL = "http://localhost:8000"

st.set_page_config(page_title="MAS Monitor", layout="wide")

st.title("🤖 Multi-Agent System Monitor")
st.markdown("### Техподдержка — мультиагентная система")

col1, col2, col3 = st.columns(3)

try:
    response = requests.get(f"{API_URL}/stats", timeout=2)
    stats = response.json()
    
    with col1:
        st.metric("📊 Всего обработано", stats.get("total_processed", 0))
    with col2:
        st.metric("⏳ В очереди", stats.get("pending_tasks", 0))
    with col3:
        st.metric("🎫 Всего тикетов", stats.get("total_tickets", 0))
        
except:
    st.error("❌ Оркестратор не отвечает. Запустите orchestrator/main.py")

st.divider()

st.subheader("🔄 Создать новый тикет")

with st.form("ticket_form"):
    title = st.text_input("Заголовок")
    description = st.text_area("Описание")
    submitted = st.form_submit_button("Отправить")
    
    if submitted and title:
        try:
            resp = requests.post(f"{API_URL}/ticket", json={
                "title": title,
                "description": description
            })
            if resp.status_code == 200:
                data = resp.json()
                st.success(f"✅ Тикет создан! ID: {data['id']}")
                st.info(f"Ответ: {data.get('response', 'Обрабатывается...')}")
            else:
                st.error(f"Ошибка: {resp.status_code}")
        except Exception as e:
            st.error(f"Ошибка подключения: {e}")

st.divider()

st.subheader("📋 Активность системы")

placeholder = st.empty()

while True:
    try:
        resp = requests.get(f"{API_URL}/stats", timeout=2)
        stats = resp.json()
        
        fig = go.Figure(data=[
            go.Bar(name="Обработано", x=["Активность"], y=[stats.get("total_processed", 0)]),
            go.Bar(name="В очереди", x=["Активность"], y=[stats.get("pending_tasks", 0)])
        ])
        fig.update_layout(title="Текущая нагрузка", height=300)
        
        placeholder.plotly_chart(fig, use_container_width=True)
        
    except:
        placeholder.warning("Ожидание подключения к оркестратору...")
    
    time.sleep(5)