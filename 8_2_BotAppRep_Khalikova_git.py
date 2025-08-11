from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import io
import telegram
import pandahouse as ph
import numpy as np

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

schedule_interval = '0 8 * * *'  # каждый день в 11 мск


# Telegram параметры
bot_token = '___'
bot = telegram.Bot(token=bot_token)
chat_id = -1002614297220 # 1023943467

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, tags=['app_report'])
def dag_botajkhalikova_appreport():
    
    @task()
    def extract_app_metrics():
        """Метрики приложения за последние 30 дней"""
        query = '''
        WITH 
        -- Метрики ленты новостей
        feed_metrics AS (
            SELECT 
                toDate(time) AS date,
                uniqExact(user_id) AS feed_dau,
                countIf(action = 'view') AS views,
                countIf(action = 'like') AS likes,
                CASE 
                    WHEN countIf(action = 'view') > 0 THEN likes / views 
                    ELSE 0 
                END AS ctr
            FROM simulator_20250520.feed_actions
            WHERE toDate(time) BETWEEN today() - 30 AND yesterday()
            GROUP BY date
        ),
        
        -- Метрики мессенджера
        message_metrics AS (
            SELECT 
                toDate(time) AS date,
                uniqExact(user_id) AS messenger_dau,
                count(*) AS messages_sent,
                uniqExact(receiver_id) AS users_received
            FROM simulator_20250520.message_actions
            WHERE toDate(time) BETWEEN today() - 30 AND yesterday()
            GROUP BY date
        ),
        
        -- Общий DAU приложения
        app_dau AS (
            SELECT 
                toDate(time) AS date,
                uniqExact(user_id) AS total_dau
            FROM (
                SELECT user_id, time FROM simulator_20250520.feed_actions
                WHERE toDate(time) BETWEEN today() - 30 AND yesterday()
                UNION ALL
                SELECT user_id, time FROM simulator_20250520.message_actions
                WHERE toDate(time) BETWEEN today() - 30 AND yesterday()
            )
            GROUP BY date
        )
        
        
        SELECT 
            a.date AS date,
            COALESCE(a.total_dau, 0) AS total_dau,
            COALESCE(f.feed_dau, 0) AS feed_dau,
            COALESCE(f.views, 0) AS views,
            COALESCE(f.likes, 0) AS likes,
            COALESCE(f.ctr, 0) AS ctr,
            COALESCE(m.messenger_dau, 0) AS messenger_dau,
            COALESCE(m.messages_sent, 0) AS messages_sent,
            COALESCE(m.users_received, 0) AS users_received
        FROM app_dau a
        LEFT JOIN feed_metrics f ON a.date = f.date
        LEFT JOIN message_metrics m ON a.date = m.date
        ORDER BY a.date
        '''
        
        df = ph.read_clickhouse(query, connection=connection)
               
        return df
    
    @task()
    def extract_yesterday_summary():
        """Сводка за вчерашний день"""
        query = '''
        WITH 
        -- Общие метрики за вчера
        yesterday_summary AS (
            SELECT 
                uniqExact(user_id) AS total_users_yesterday,
                uniqExactIf(user_id, source = 'organic') AS organic_users,
                uniqExactIf(user_id, source = 'ads') AS ads_users
            FROM (
                SELECT user_id, source FROM simulator_20250520.feed_actions
                WHERE toDate(time) = yesterday()
                UNION ALL
                SELECT user_id, source FROM simulator_20250520.message_actions
                WHERE toDate(time) = yesterday()
            )
        ),
        
        -- Метрики ленты за вчера
        feed_yesterday AS (
            SELECT 
                uniqExact(user_id) AS feed_dau,
                countIf(action = 'view') AS views,
                countIf(action = 'like') AS likes,
                CASE 
                    WHEN countIf(action = 'view') > 0 THEN likes / views 
                    ELSE 0 
                END AS ctr
            FROM simulator_20250520.feed_actions
            WHERE toDate(time) = yesterday()
        ),
        
        -- Метрики мессенджера за вчера
        message_yesterday AS (
            SELECT 
                uniqExact(user_id) AS messenger_dau,
                count(*) AS messages_sent,
                uniqExact(receiver_id) AS users_received
            FROM simulator_20250520.message_actions
            WHERE toDate(time) = yesterday()
        )
        
        SELECT 
            COALESCE(ys.total_users_yesterday, 0) AS total_users_yesterday,
            COALESCE(ys.organic_users, 0) AS organic_users,
            COALESCE(ys.ads_users, 0) AS ads_users,
            COALESCE(fy.feed_dau, 0) AS feed_dau,
            COALESCE(fy.views, 0) AS views,
            COALESCE(fy.likes, 0) AS likes,
            COALESCE(fy.ctr, 0) AS ctr,
            COALESCE(my.messenger_dau, 0) AS messenger_dau,
            COALESCE(my.messages_sent, 0) AS messages_sent,
            COALESCE(my.users_received, 0) AS users_received
        FROM yesterday_summary ys
        FULL OUTER JOIN feed_yesterday fy ON 1=1
        FULL OUTER JOIN message_yesterday my ON 1=1
        '''
        
        df = ph.read_clickhouse(query, connection=connection)
        
        return df
    
    @task()
    def send_text_report(app_metrics, yesterday_summary):
        """Отправка текстового отчета"""
        
        yesterday_data = yesterday_summary.iloc[0]
        yesterday = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)
        report_date = yesterday.strftime('%d.%m.%Y')
        
        def safe_int(value, default=0):
            try:
                return int(value) if pd.notna(value) else default
            except:
                return default
        
        def safe_float(value, default=0.0):
            try:
                return float(value) if pd.notna(value) else default
            except:
                return default
        
        messages_per_user = safe_float(yesterday_data['messages_sent']/yesterday_data['messenger_dau'] if yesterday_data['messenger_dau'] != 0 else 0)
        avg_dau = app_metrics['total_dau'].mean() if not app_metrics.empty else 0
        avg_ctr = app_metrics['ctr'].mean() if not app_metrics.empty else 0.0
        avg_messages = app_metrics['messages_sent'].mean() if not app_metrics.empty else 0
        
        msg = f"""
📊 **Комплексный отчет по приложению за {report_date}**

🏢 **Общие метрики приложения:**
• Общий DAU: {safe_int(yesterday_data['total_users_yesterday']):,}
• Пользователи из organic: {safe_int(yesterday_data['organic_users']):,}
• Пользователи из ads: {safe_int(yesterday_data['ads_users']):,}

📰 **Лента новостей:**
• DAU ленты: {safe_int(yesterday_data['feed_dau']):,}
• Просмотры: {safe_int(yesterday_data['views']):,}
• Лайки: {safe_int(yesterday_data['likes']):,}
• CTR: {safe_float(yesterday_data['ctr']):.3f}

💬 **Мессенджер:**
• DAU мессенджера: {safe_int(yesterday_data['messenger_dau']):,}
• Отправлено сообщений: {safe_int(yesterday_data['messages_sent']):,}
• Получатели: {safe_int(yesterday_data['users_received']):,}
• Сообщений на пользователя: {messages_per_user:.2f}

📈 **Динамика за неделю:**
• Средний DAU: {avg_dau:.0f}
• Средний CTR: {avg_ctr:.3f}
• Среднее сообщений/день: {avg_messages:.0f}
        """
        
        bot.send_message(chat_id=chat_id, text=msg, parse_mode='Markdown')
    
    @task()
    def send_comprehensive_report_new(app_metrics, detailed_data):
        """Создаание и отправка комплексного отчета"""

        if app_metrics.empty:
            bot.send_message(chat_id=chat_id, text="❌ Нет данных для комплексного отчета (app_metrics)")
            return
        if not detailed_data or detailed_data['source'].empty:
            bot.send_message(chat_id=chat_id, text="❌ Нет данных для комплексного отчета (source_data)")
            return

        if 'a.date' in app_metrics.columns:
            app_metrics = app_metrics.rename(columns={'a.date': 'date'})
        app_metrics['date'] = pd.to_datetime(app_metrics['date'])
        
        source_df = detailed_data['source']
        source_df['date'] = pd.to_datetime(source_df['date'])

        # График 
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Комплексный анализ приложения', fontsize=16, fontweight='bold')

        # 1. График динамики DAU 
        if 'total_dau' in app_metrics.columns:
            axes[0, 0].plot(app_metrics['date'], app_metrics['total_dau'], marker='o', linewidth=2, color='blue')
            axes[0, 0].set_title('Динамика DAU', fontweight='bold')
            axes[0, 0].set_ylabel('Количество пользователей')
            axes[0, 0].tick_params(axis='x', rotation=45)
            axes[0, 0].grid(True, alpha=0.3)
        else:
            axes[0, 0].text(0.5, 0.5, 'Нет данных\nпо DAU', ha='center', va='center', transform=axes[0, 0].transAxes)
            axes[0, 0].set_title('Динамика DAU', fontweight='bold')

        # 2. График динамики CTR
        if 'ctr' in app_metrics.columns:
            axes[0, 1].plot(app_metrics['date'], app_metrics['ctr']*100, marker='s', linewidth=2, color='green')
            axes[0, 1].set_title('Динамика CTR', fontweight='bold')
            axes[0, 1].set_ylabel('CTR (%)')
            axes[0, 1].tick_params(axis='x', rotation=45)
            axes[0, 1].grid(True, alpha=0.3)
        else:
            axes[0, 1].text(0.5, 0.5, 'Нет данных\nпо CTR', ha='center', va='center', transform=axes[0, 1].transAxes)
            axes[0, 1].set_title('Динамика CTR', fontweight='bold')
            
        # 3. График динамики пользователей по источникам
        pivot_source = source_df.pivot_table(
            values='users_from_source', 
            index='date', 
            columns='source', 
            aggfunc='sum',
            fill_value=0
        )
        for source in pivot_source.columns:
            axes[1, 0].plot(pivot_source.index, pivot_source[source], marker='o', label=source, linewidth=2)
        axes[1, 0].set_title('Динамика пользователей по источникам', fontweight='bold')
        axes[1, 0].set_ylabel('Количество пользователей')
        axes[1, 0].legend()
        axes[1, 0].tick_params(axis='x', rotation=45)
        axes[1, 0].grid(True, alpha=0.3)
        
        # 4. График динамики сообщений в день
        if 'messages_sent' in app_metrics.columns:
            axes[1, 1].plot(app_metrics['date'], app_metrics['messages_sent'], marker='^', linewidth=2, color='purple')
            axes[1, 1].set_title('Динамика сообщений в день', fontweight='bold')
            axes[1, 1].set_ylabel('Количество сообщений')
            axes[1, 1].tick_params(axis='x', rotation=45)
            axes[1, 1].grid(True, alpha=0.3)
        else:
            axes[1, 1].text(0.5, 0.5, 'Нет данных\nпо сообщениям', ha='center', va='center', transform=axes[1, 1].transAxes)
            axes[1, 1].set_title('Динамика сообщений в день', fontweight='bold')

        plt.tight_layout()

        # Отправка
        plot_object = io.BytesIO()
        plt.savefig(plot_object, format='png', dpi=300, bbox_inches='tight')
        plot_object.seek(0)
        plot_object.name = 'app_comprehensive_report_new.png'
        plt.close()
        
        bot.send_photo(chat_id=chat_id, photo=plot_object, caption='Комплексный анализ приложения')
    
    @task()
    def send_heatmap(detailed_data):
        """Создание и отправка тепловой карты активности"""
        
        if not detailed_data or detailed_data['hourly'].empty:
            msg = "❌ Нет данных для тепловой карты"
            bot.send_message(chat_id=chat_id, text=msg)
            return
        
        # Тепловая карта активности по часам
        hourly_df = detailed_data['hourly']
        hourly_df['date'] = pd.to_datetime(hourly_df['date'])
        hourly_df['weekday'] = hourly_df['date'].dt.dayofweek
        
        heatmap_data = hourly_df.pivot_table(
            values='hourly_dau', 
            index='weekday', 
            columns='hour', 
            aggfunc='mean',
            fill_value=0
        )
        
        fig, ax = plt.subplots(1, 1, figsize=(15, 8))
        sns.heatmap(heatmap_data, annot=True, fmt='.0f', cmap='YlOrRd', ax=ax)
        ax.set_title('Тепловая карта активности по часам и дням недели', fontweight='bold', fontsize=14)
        ax.set_xlabel('Час дня')
        ax.set_ylabel('День недели')
        ax.set_yticklabels(['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'])
        
        plt.tight_layout()
        
        plot_object = io.BytesIO()
        plt.savefig(plot_object, format='png', dpi=300, bbox_inches='tight')
        plot_object.seek(0)
        plot_object.name = 'heatmap.png'
        plt.close()
        
        bot.send_photo(chat_id=chat_id, photo=plot_object, caption='Тепловая карта активности по часам')
    
    @task()
    def send_period_comparison(app_metrics):
        """Создание и отправка графика сравнения периодов"""
        
        if len(app_metrics) < 28:
            msg = "ℹ️ Недостаточно данных для сравнения периодов (нужно минимум 28 дней)"
            bot.send_message(chat_id=chat_id, text=msg)
            return

        fig, axes = plt.subplots(1, 2, figsize=(16, 8))
        fig.suptitle('Сравнение периодов (1-я и 2-я половина месяца)', fontsize=16, fontweight='bold')
        
        first_half = app_metrics.iloc[:len(app_metrics)//2]
        second_half = app_metrics.iloc[len(app_metrics)//2:]
        
        # Сравнение DAU
        if 'total_dau' in app_metrics.columns:
            current_dau = second_half['total_dau'].mean()
            previous_dau = first_half['total_dau'].mean()
            change_pct = ((current_dau - previous_dau) / previous_dau * 100) if previous_dau > 0 else 0
            
            periods = ['1-я половина', '2-я половина']
            values = [previous_dau, current_dau]
            colors = ['lightcoral', 'lightblue']
            
            bars = axes[0].bar(periods, values, color=colors, alpha=0.8)
            axes[0].set_title(f'Сравнение DAU\nИзменение: {change_pct:+.1f}%', fontweight='bold')
            axes[0].set_ylabel('Средний DAU')
            axes[0].grid(True, alpha=0.3)
            
            for bar, value in zip(bars, values):
                axes[0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(values)*0.01, 
                             f'{value:.0f}', ha='center', va='bottom')
        
        # Сравнение CTR
        if 'ctr' in app_metrics.columns:
            current_ctr = second_half['ctr'].mean()
            previous_ctr = first_half['ctr'].mean()
            change_pct_ctr = ((current_ctr - previous_ctr) / previous_ctr * 100) if previous_ctr > 0 else 0
            
            periods = ['1-я половина', '2-я половина']
            values_ctr = [previous_ctr*100, current_ctr*100]
            colors = ['lightcoral', 'lightblue']
            
            bars = axes[1].bar(periods, values_ctr, color=colors, alpha=0.8)
            axes[1].set_title(f'Сравнение CTR\nИзменение: {change_pct_ctr:+.1f}%', fontweight='bold')
            axes[1].set_ylabel('Средний CTR (%)')
            axes[1].grid(True, alpha=0.3)
            
            for bar, value in zip(bars, values_ctr):
                axes[1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(values_ctr)*0.01, 
                             f'{value:.3f}%', ha='center', va='bottom')
        
        plt.tight_layout()
        
        plot_object = io.BytesIO()
        plt.savefig(plot_object, format='png', dpi=300, bbox_inches='tight')
        plot_object.seek(0)
        plot_object.name = 'period_comparison.png'
        plt.close()
        
        bot.send_photo(chat_id=chat_id, photo=plot_object, caption='📊 Сравнение периодов')
    
    @task()
    def extract_detailed_analytics_data():
        """Извлекаем детальные данные для отчетов"""
        
        # Сначала проверим структуру таблицы
        try:
            structure_query = "DESCRIBE simulator_20250520.feed_actions"
            structure_data = ph.read_clickhouse(structure_query, connection=connection)
            print(f"Структура таблицы feed_actions: {structure_data}")
            
            # Определяем правильное название колонки времени
            time_column = 'time'  # по умолчанию
            if not structure_data.empty:
                columns = structure_data.iloc[:, 0].tolist()
                if 'timestamp' in columns:
                    time_column = 'timestamp'
                elif 'time' in columns:
                    time_column = 'time'
                elif 'event_time' in columns:
                    time_column = 'event_time'
                print(f"Используем колонку времени: {time_column}")
        except Exception as e:
            print(f"Ошибка при проверке структуры таблицы: {e}")
            time_column = 'time'  # используем по умолчанию
        
        try:
            # 1. Данные по часам для тепловой карты
            hourly_query = f'''
            SELECT 
                toDate({time_column}) AS date,
                toHour({time_column}) AS hour,
                uniqExact(user_id) AS hourly_dau
            FROM simulator_20250520.feed_actions
            WHERE toDate({time_column}) BETWEEN today() - 30 AND yesterday()
            GROUP BY date, hour
            ORDER BY date, hour
            '''
            
            hourly_data = ph.read_clickhouse(hourly_query, connection=connection)
        except Exception as e:
            print(f"Ошибка при получении почасовых данных: {e}")
            hourly_data = pd.DataFrame()
        
        try:
            # 2. Данные по источникам трафика
            source_query = f'''
            SELECT 
                toDate({time_column}) AS date,
                source,
                uniqExact(user_id) AS users_from_source
            FROM simulator_20250520.feed_actions
            WHERE toDate({time_column}) BETWEEN today() - 30 AND yesterday()
            GROUP BY date, source
            ORDER BY date, source
            '''
            
            source_data = ph.read_clickhouse(source_query, connection=connection)
        except Exception as e:
            print(f"Ошибка при получении данных по источникам: {e}")
            source_data = pd.DataFrame()
        
        return {
            'hourly': hourly_data,
            'source': source_data
        }
    
    # Выполнение задач
    app_metrics = extract_app_metrics()
    yesterday_summary = extract_yesterday_summary()
    detailed_data = extract_detailed_analytics_data()
    
    send_text_report(app_metrics, yesterday_summary)
    send_comprehensive_report_new(app_metrics, detailed_data)
    send_heatmap(detailed_data)
    send_period_comparison(app_metrics)


dag_botajkhalikova_appreport = dag_botajkhalikova_appreport() 
