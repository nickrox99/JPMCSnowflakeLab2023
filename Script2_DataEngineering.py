##### Welcome to Snowpark Using the Snowflake Python Worksheets
#You can now write Snowpark code in Python worksheets to process data using Snowpark for Python in Snowsight. With this new experience, you can perform your development and testing in Snowflake, without needing to install dependent libraries. It’s important to note that this compliments notebooks and other development environments. You are able to connect to Snowflake with Snowpark from your favorite Notebook and IDEs. Learn more about setting up your development environment in Snowflake Documentation: https://docs.snowflake.com/en/developer-guide/snowpark/python/setup

#In this worksheet, you can read through to understand core concepts of working in Python with Snowpark and run the entire worksheet:

# * Load data from Snowflake tables into Snowpark DataFrames
# * Perform exploratory data analysis on Snowpark DataFrames
# * Pivot and join data from multiple tables using Snowpark DataFrames
# * Save transformed data into a Snowflake table

# Import Snowpark for Python library and the DataFrame functions we will use in this Worksheet
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import month,year,col,sum

def main(session: snowpark.Session):
    # What is a Snowpark DataFrame
    # It represents a lazily-evaluated relational dataset that contains a collection of Row objects with columns defined by a schema (column name and type). Here are some ways to load data in a Snowpark DataFrame:
    # - session.table('table_name')
    # - session.sql("select col1, col2... from tableName")*
    # - session.read.options({"field_delimiter": ",", "skip_header": 1}).schema(user_schema).csv("@mystage/testCSV.csv")*
    # - session.read.parquet("@stageName/path/to/file")*
    # - session.create_dataframe([1,2,3], schema=["col1"])*

    ### Load Data from Snowflake tables into Snowpark DataFrames
    # Let's load the campaign spend and revenue data. This campaign spend table contains ad click data that has been aggregated to show daily spend across digital ad channels including search engines, social media, email and video. The revenue table contains revenue data for 10yrs.

    snow_df_spend = session.table('campaign_spend')
    snow_df_revenue = session.table('monthly_revenue')

    ### Total Spend per Year and Month For All Channels
    # Let's transform the campaign spend data so we can see total cost per year/month per channel using _group_by()_ and _agg()_ Snowpark DataFrame functions.

    snow_df_spend_per_channel = snow_df_spend.group_by(year('DATE'), month('DATE'),'CHANNEL').agg(sum('TOTAL_COST').as_('TOTAL_COST')).\
    with_column_renamed('"YEAR(DATE)"',"YEAR").with_column_renamed('"MONTH(DATE)"',"MONTH").sort('YEAR','MONTH')

    # See the output of “print()” and “show()” in the "Output" tab below
    print("Total Spend per Year and Month For All Channels")
    snow_df_spend_per_channel.show()

    ### Total Spend Across All Channels
    # Let's further transform the campaign spend data so that each row represents total cost across all channels per year/month using the pivot() and sum() Snowpark DataFrame functions.
    # This transformation lets us join with the revenue table so that our input features and target variable will be in a single table for model training.

    snow_df_spend_per_month = snow_df_spend_per_channel.pivot('CHANNEL',['search_engine','social_media','video','email']).sum('TOTAL_COST').sort('YEAR','MONTH')
    snow_df_spend_per_month = snow_df_spend_per_month.select(
        col("YEAR"),
        col("MONTH"),
        col("'search_engine'").as_("SEARCH_ENGINE"),
        col("'social_media'").as_("SOCIAL_MEDIA"),
        col("'video'").as_("VIDEO"),
        col("'email'").as_("EMAIL")
    )

    # See the output of “print()” and “show()” in the "Output" tab below
    print("Total Spend Across All Channels")
    snow_df_spend_per_month.show()

    ### Total Revenue per Year and Month
    # Now let's transform the revenue data into revenue per year/month using group_by() and agg() functions.

    snow_df_revenue_per_month = snow_df_revenue.group_by('YEAR','MONTH').agg(sum('REVENUE')).sort('YEAR','MONTH').with_column_renamed('SUM(REVENUE)','REVENUE')

    # See the output of “print()” and “show()” in the "Output" tab below
    print("Total Revenue per Year and Month")
    snow_df_revenue_per_month.show()

    ### Join Total Spend and Total Revenue per Year and Month Across All Channels
    # Next let's join this revenue data with the transformed campaign spend data so that our input features (i.e. cost per channel) and target variable (i.e. revenue) can be loaded into a single table for model training.
    snow_df_spend_and_revenue_per_month = snow_df_spend_per_month.join(snow_df_revenue_per_month, ["YEAR","MONTH"])

    # See the output of “print()” and “show()” in the "Output" tab below
    print("Total Spend and Revenue per Year and Month Across All Channels")
    snow_df_spend_and_revenue_per_month.show()

    ### Examine Query Explain Plan
    # Snowpark makes it really convenient to look at the DataFrame query and execution plan using explain() Snowpark DataFrame function.
    # See the output of “explain()” in the "Output" tab below
    snow_df_spend_and_revenue_per_month.explain()

    ### Save Transformed Data into Snowflake Table
    # Let's save the transformed data into a Snowflake table *SPEND_AND_REVENUE_PER_MONTH*

    snow_df_spend_and_revenue_per_month.write.mode('overwrite').save_as_table('SPEND_AND_REVENUE_PER_MONTH')

    # See the output of this in "Results" tab below
    return snow_df_spend_and_revenue_per_month

