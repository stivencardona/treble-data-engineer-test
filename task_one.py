import matplotlib.pyplot as plt
import redshift_connector
import datetime


conn = redshift_connector.connect(
     host='redshift-cluster-datawarehouse.cew9a5azwld4.us-east-1.redshift.amazonaws.com',
     database='test',
     port=5439,
     user='user_test',
     password='Password123'
  )

cursor = conn.cursor()

def get_activatation_date(threshold: int):
    raw_sql = f'''
        with conversations_per_company as ( 
	        select sum(c.total) as total, c.date, cmpi.company_id from conversations c inner join company_identifiers cmpi on cmpi.account_identifier = c.account_id join company cmp on cmp.id = cmpi.company_id where cmp.associated_partner != '' group by c.date, cmpi.company_id
        ), conversations_per_activation_interval as (
	        select cmp.date, cmp.company_id, SUM(cmp.total) OVER(PARTITION BY cmp.company_id ORDER BY cmp.date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS total_conversations FROM conversations_per_company cmp
        ), activate_date_by_company as (
	        select company_id, first_value(date) over ( partition by company_id rows between unbounded preceding and unbounded following) activation_date from conversations_per_activation_interval where conversations_per_activation_interval.total_conversations >= {threshold} order by date
        ) select company_id, activation_date from activate_date_by_company group by company_id, activation_date order by activation_date asc
    '''
    cursor.execute(raw_sql)
    return cursor.fetchall()

def percentage_by_month(successfull_conversations: tuple, threshold: int):
    if not len(successfull_conversations):
        return 0.0
    return (len([x for x in successfull_conversations if(x[0] > threshold)]) * 100) / len(successfull_conversations) 

# Generate dates for the plot
def get_date_with_months(intial_year: int, intial_month: int, months: int):
    months_summary = intial_month + months
    calculated_month = months_summary % 12 if months_summary != 12 else 12
    calculated_year = intial_year + (months_summary // 12 ) - (months_summary % 12 == 0)
    return (calculated_year, calculated_month)

def generate_dates():
    year = 2023
    start_month = 1
    end_month = 10

    generated_dates = []

    for delta_months in range(end_month - start_month + 1):
        curr_year, curr_month = get_date_with_months(year, start_month, delta_months)
        next_year, next_month = get_date_with_months(year, start_month, delta_months + 1)
        generated_dates.append((datetime.datetime(curr_year, curr_month, 1).strftime('%Y-%m-%d'),datetime.datetime(next_year, next_month, 1).strftime('%Y-%m-%d')))

    return generated_dates

def generate_graph_task_two():
    x = []
    y = []
    for curr_date, next_date in generate_dates():
        x.append(curr_date)
        y.append(percentage_by_month(get_data_by_month(next_date, curr_date), 1500))

    plt.figure(figsize = (15, 10))
    plt.grid(which='major', axis='both', linestyle='--', color='gray', linewidth=1)
    plt.plot(x,y, 'go-')
    plt.title("Percentage of successfull companies over time ")
    plt.xlabel('Dates')
    plt.ylabel('Percentage')

    plt.savefig("task_two.png")
