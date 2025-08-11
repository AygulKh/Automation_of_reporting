from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import io
import telegram
import pandahouse as ph

from airflow.decorators import dag, task

# Подключение к ClickHouse
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '',
    'user': 'student',
    'database': 'simulator_20250520'
}

# Параметры DAG
default_args = {
    'owner': 'aj-halikova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 6, 27)
}

schedule_interval = '0 8 * * *'  # в 11 мск каждый день

# Telegram параметры
bot_token = '___'
bot = telegram.Bot(token=bot_token)
chat_id = -1002614297220 # 1023943467

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_botajkhalikova_report():
    @task()
    def extract_metrics():
        query = '''
        SELECT 
            toDate(time) AS date,
            uniqExact(user_id) AS dau,
            countIf(action='view') as views,
            countIf(action='like') as likes,
            (likes/views)*100 as ctr
        FROM simulator_20250520.feed_actions
        WHERE toDate(time) >= today() - 7
          AND toDate(time) <= yesterday()
        GROUP BY date
        ORDER BY date
        '''
        df = ph.read_clickhouse(query, connection=connection)
        return df

    @task()
    def send_text_report(df):
        # Данные за вчера
        yesterday = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)
        df_y = df[df['date'] == yesterday]
        if not df_y.empty:
            msg = (
                f"\U0001F4C8 <b>Отчет по ленте за {df_y.date.iloc[0].date()}:</b>\n"
                f"<b>DAU:</b> {df_y.dau.iloc[0]}\n"
                f"<b>Просмотры:</b> {df_y.views.iloc[0]}\n"
                f"<b>Лайки:</b> {df_y.likes.iloc[0]}\n"
                f"<b>CTR:</b> {df_y.ctr.iloc[0]:.3f}%"
            )
            bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
        else:
            msg = f"Нет данных за {yesterday.strftime('%d.%m.%Y')}"
            bot.send_message(chat_id=chat_id, text=msg)

    @task()
    def send_plot_report(df):
        sns.set_theme(style="whitegrid")
        plt.figure(figsize=(16, 10))
        # DAU
        plt.subplot(2, 2, 1)
        sns.lineplot(data=df, x='date', y='dau', marker='o')
        plt.title('DAU за 7 дней')
        plt.xlabel('Дата')
        plt.ylabel('DAU')
        plt.xticks(rotation=45)
        # Views
        plt.subplot(2, 2, 4)
        sns.lineplot(data=df, x='date', y='views', marker='o', color='orange')
        plt.title('Просмотры за 7 дней')
        plt.xlabel('Дата')
        plt.ylabel('Просмотры')
        plt.xticks(rotation=45)
        # Likes
        plt.subplot(2, 2, 3)
        sns.lineplot(data=df, x='date', y='likes', marker='o', color='green')
        plt.title('Лайки за 7 дней')
        plt.xlabel('Дата')
        plt.ylabel('Лайки')
        plt.xticks(rotation=45)
        # CTR
        plt.subplot(2, 2, 2)
        sns.lineplot(data=df, x='date', y='ctr', marker='o', color='red')
        plt.title('CTR за 7 дней')
        plt.xlabel('Дата')
        plt.ylabel('CTR')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'metrics_report.png'
        plt.close()
        bot.send_photo(chat_id=chat_id, photo=plot_object)

    df = extract_metrics()
    send_text_report(df)
    send_plot_report(df)


dag_botajkhalikova_report = dag_botajkhalikova_report() 
