import matplotlib.pyplot as plt
import redshift_connector
import datetime
from dateutil.relativedelta import relativedelta 


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

def get_cummulative_conversations_by_week(companies_ids: list[int]):
    raw_sql = f'''
        with cummulative_successful_conversations as (
            select sum(c.total) as total, date_trunc('week', c.date)::date as week, cmp.company_id from conversations c inner join company_identifiers cmp on cmp.account_identifier = c.account_id where company_id in ({str(companies_ids)[1:-1]}) and successful = true group by c.date, cmp.company_id order by week asc
        ) select sum(total), week, company_id as successful_conversations from cummulative_successful_conversations group by week, company_id order by week asc
    '''
    cursor.execute(raw_sql)
    return cursor.fetchall()


def amount_count_by_company(company_id, curr_date, activation_dates):
    

    if(curr_date >= activation_dates[company_id] and curr_date <= activation_dates[company_id] + relativedelta(months=2)):
        return True
    
    return False

def flat_weeks_per_company():
    companies_with_activate_date = get_activatation_date(350)
    activation_dates = {}
    companies_ids =  [company[0] for company in companies_with_activate_date]
    for company in companies_with_activate_date:
        companies_ids.append(company[0])
        activation_dates[company[0]] = company[1]

    data = get_cummulative_conversations_by_week(companies_ids)
    weeks = []
    successfull_company = []
    cummulative_successful_companies = 0
    cummulative_by_companies = {}
    threshold_state_by_company = {}
    for x in data:
        amount, week_of_data, company_id = x
        if amount_count_by_company(company_id, week_of_data, activation_dates):
            if company_id in cummulative_by_companies: 
                cummulative_by_companies[company_id] += amount 
            else:
                cummulative_by_companies[company_id] = amount

            if cummulative_by_companies[company_id] > 500 and not company_id in threshold_state_by_company:
                threshold_state_by_company[company_id] = True
                cummulative_successful_companies += 1

        weeks.append(week_of_data.strftime('%Y-%m-%d'))
        successfull_company.append(cummulative_successful_companies)
    
    result = {
        'weeks': [],
        'successfull_companies': []
    }

    idx = 0
    while idx < len(weeks) - 1:
        while(weeks[idx] == weeks[idx + 1]):
            idx += 1
        result['weeks'].append(weeks[idx])
        result['successfull_companies'].append(successfull_company[idx])
        idx += 1
    result['weeks'].append(weeks[idx])
    result['successfull_companies'].append(successfull_company[idx])

    return result

def generate_graph_task_one():
    data = flat_weeks_per_company()
    x = data['weeks']
    y = data['successfull_companies']

    plt.figure(figsize = (20, 10))
    plt.grid(which='major', axis='both', linestyle='--', color='gray', linewidth=1)
    plt.plot(x,y, 'go-')
    plt.title("Cummulative successfull companies over time ")
    plt.xlabel('Weeks')
    plt.xticks(rotation=90)
    plt.ylabel('Cummulative')

    plt.savefig("task_one.png")
    
generate_graph_task_one()