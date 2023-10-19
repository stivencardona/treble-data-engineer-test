import matplotlib.pyplot as plt
import matplotlib
import redshift_connector
import datetime
import numpy as np

conn = redshift_connector.connect(
     host='redshift-cluster-datawarehouse.cew9a5azwld4.us-east-1.redshift.amazonaws.com',
     database='test',
     port=5439,
     user='user_test',
     password='Password123'
  )

cursor = conn.cursor()

def get_data_by_month(next_date: str, current_date: str):
    raw_sql = f'''
        with recursive references_by_company as (
	        select regexp_count(stripe_company_ids, ',') + 1 as reference_number, company_id, stripe_company_ids  as stripe_ids from company_identifiers where stripe_company_ids != '' group by company_id, stripe_company_ids
        ), stripe_reference (company_id, idx, stripe_id) as (
	        select company_id, 1 as idx, split_part(stripe_ids, ',', 1) as stripe_id from references_by_company
	        union all 
	        select stripe_reference.company_id, idx + 1 as idx, split_part(stripe_ids, ',', idx + 1) as stripe_id from references_by_company join stripe_reference on stripe_reference.company_id = references_by_company.company_id  where idx < reference_number
        ), comapny_by_close_date as (
	        select cpn.close_date, cpn.id, sr.stripe_id, cpn.name from company cpn join stripe_reference sr on cpn.id = sr.company_id where cpn.close_date >= '{current_date}' and cpn.close_date < '{next_date}'
        ) select extract(month from sri.sent_date) as invoice_date, sum(amount) from stripe_invoice sri join comapny_by_close_date cpn_date on cpn_date.stripe_id = sri.company_id group by invoice_date order by invoice_date asc
    '''
    cursor.execute(raw_sql)
    return cursor.fetchall()

# Generate dates for the plot
def get_date_with_months(intial_year: int, intial_month: int, months: int):
    months_summary = intial_month + months
    calculated_month = months_summary % 12 if months_summary != 12 else 12
    calculated_year = intial_year + (months_summary // 12 ) - (months_summary % 12 == 0)
    return (calculated_year, calculated_month)

def generate_dates(year: int, start_month: int, end_month):

    generated_dates = []

    for delta_months in range(end_month - start_month + 1):
        curr_year, curr_month = get_date_with_months(year, start_month, delta_months)
        next_year, next_month = get_date_with_months(year, start_month, delta_months + 1)
        generated_dates.append((datetime.datetime(curr_year, curr_month, 1).strftime('%Y-%m-%d'),datetime.datetime(next_year, next_month, 1).strftime('%Y-%m-%d')))

    return generated_dates

def heatmap(data, row_labels, ax=None,
            cbar_kw={}, cbarlabel="", **kwargs):
    """
    Create a heatmap from a numpy array and two lists of labels.

    Parameters
    ----------
    data
        A 2D numpy array of shape (N, M).
    row_labels
        A list or array of length N with the labels for the rows.
    col_labels
        A list or array of length M with the labels for the columns.
    ax
        A `matplotlib.axes.Axes` instance to which the heatmap is plotted.  If
        not provided, use current axes or create a new one.  Optional.
    cbar_kw
        A dictionary with arguments to `matplotlib.Figure.colorbar`.  Optional.
    cbarlabel
        The label for the colorbar.  Optional.
    **kwargs
        All other arguments are forwarded to `imshow`.
    """

    if not ax:
        ax = plt.gca()

    # Plot the heatmap
    im = ax.imshow(data, **kwargs)

    # Create colorbar
    cbar = ax.figure.colorbar(im, ax=ax, **cbar_kw)
    cbar.ax.set_ylabel(cbarlabel, rotation=-90, va="bottom")

    # We want to show all ticks...
    ax.set_xticks(np.arange(data.shape[1]))
    ax.set_yticks(np.arange(data.shape[0]))
    # ... and label them with the respective list entries.
    ax.set_yticklabels(row_labels)

    # Let the horizontal axes labeling appear on top.
    ax.tick_params(top=False, bottom=False,
                   labeltop=False, labelbottom=False)

    # Rotate the tick labels and set their alignment.
    plt.setp(ax.get_xticklabels(), rotation=-30, ha="right",
             rotation_mode="anchor")

    # Turn spines off and create white grid.
    for edge, spine in ax.spines.items():
        spine.set_visible(False)

    ax.set_xticks(np.arange(data.shape[1]+1)-.5, minor=True)
    ax.set_yticks(np.arange(data.shape[0]+1)-.5, minor=True)
    ax.grid(which="minor", color="w", linestyle='-', linewidth=3)
    ax.tick_params(which="minor", bottom=False, left=False)

    return im, cbar


def annotate_heatmap(im, data=None, valfmt="{x:.2f}",
                     textcolors=["black", "white"],
                     threshold=None, **textkw):
    """
    A function to annotate a heatmap.

    Parameters
    ----------
    im
        The AxesImage to be labeled.
    data
        Data used to annotate.  If None, the image's data is used.  Optional.
    valfmt
        The format of the annotations inside the heatmap.  This should either
        use the string format method, e.g. "$ {x:.2f}", or be a
        `matplotlib.ticker.Formatter`.  Optional.
    textcolors
        A list or array of two color specifications.  The first is used for
        values below a threshold, the second for those above.  Optional.
    threshold
        Value in data units according to which the colors from textcolors are
        applied.  If None (the default) uses the middle of the colormap as
        separation.  Optional.
    **kwargs
        All other arguments are forwarded to each call to `text` used to create
        the text labels.
    """

    if not isinstance(data, (list, np.ndarray)):
        data = im.get_array()

    # Normalize the threshold to the images color range.
    if threshold is not None:
        threshold = im.norm(threshold)
    else:
        threshold = im.norm(data.max())/2.

    # Set default alignment to center, but allow it to be
    # overwritten by textkw.
    kw = dict(horizontalalignment="center",
              verticalalignment="center")
    kw.update(textkw)

    # Get the formatter in case a string is supplied
    if isinstance(valfmt, str):
        valfmt = matplotlib.ticker.StrMethodFormatter(valfmt)

    # Loop over the data and create a `Text` for each "pixel".
    # Change the text's color depending on the data.
    texts = []
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            kw.update(color=textcolors[int(im.norm(data[i, j]) > threshold)])
            text = im.axes.text(j, i, valfmt(data[i, j], None), **kw)
            texts.append(text)

    return texts

def generate_graph_task_three(delta):
    dates = generate_dates(2023, 1,delta)
    
    months = [datetime.datetime.strptime(x[0], '%Y-%m-%d').strftime('%Y-%m') for x in dates]
    query_result = []
    
    for date in dates:
        query_result.append(get_data_by_month(date[1], date[0]))

    invoices = []
    number_row = 0
    for row_by_months in query_result:
        invoices.append([0] * delta)
        idx = 0
        for month_data in row_by_months:
           if month_data[0] - 1 >= number_row:
               invoices[number_row][idx] = month_data[1]
               idx += 1
        number_row += 1

    harvest = np.array(invoices)

    fig, ax = plt.subplots()

    im, cbar = heatmap(harvest, months, ax=ax,
                    cmap="YlGn", cbarlabel="Invoice ammount")
    annotate_heatmap(im, valfmt="{x}")

    fig.tight_layout()
    plt.savefig("task_three.png")

generate_graph_task_three(8)