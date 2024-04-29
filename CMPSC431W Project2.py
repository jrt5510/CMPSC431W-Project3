import sqlite3

def cli_interface():
    print('Please select an option:')
    print('1. Insert Data')
    print('2. Delete Data')
    print('3. Update Data')
    print('4. Search Data')
    print('5. Aggregate Functions')
    print('6. Sorting')
    print('7. Joining')
    print('8. Grouping')
    print('9. Subqueires')
    print('10. Exit')
    print('Please input your selection:')
    option = input()
    return option

def insert_data():
    wild_MSW_number = 0
    wild_MSW_weight = 0
    wild_1SW_number = 0
    wild_1SW_weight = 0
    sea_trout_number = 0
    sea_trout_weight = 0
    finnock_number = 0
    finnock_weight = 0
    farmed_MSW_number = 0
    farmed_MSW_weight = 0
    farmed_1SW_number = 0
    farmed_1SW_weight = 0
    district_id = input("Please enter district id:")
    cur.execute("SELECT district FROM District_info WHERE district_id = ?", (district_id,))
    district = cur.fetchone()[0]
    cur.execute("SELECT region FROM District_info WHERE district_id = ?", (district_id,))
    region = cur.fetchone()[0]
    cur.execute("SELECT report_order FROM District_info WHERE district_id = ?", (district_id,))
    report_order = cur.fetchone()[0]
    year = input("Please enter your year:")
    month_number = input("Please enter your month:")
    cur.execute('''SELECT month FROM MonthNo WHERE month_number = {}'''.format(month_number))
    month = cur.fetchone()[0]
    method_number = input("Please select catching method:\n1. Fixed Engine: Retained\n2. Net and Coble: Retained\n")
    method = None
    if method_number == '1':
        method = 'Fixed Engine: Retained'
    elif method_number == '2':
        method = 'Net and Coble: Retained'
    else:
        print("Invalid method selected. Defaulting to 'Fixed Engine: Retained'")
        method = 'Fixed Engine: Retained'
    if method is None:
        print("No method assigned, cancelling operation.")
        return
    netting_effort = input("Please enter netting effort:")
    fish_selection = str(input("Please select fish types(e.g., '1,2 for wild MSW salmon and wild 1SW salmon):\n1. Wild MSW Salmon\n2. Wild 1SW Salmon\n3. Sea Trout\n4. Finnock\n5. Farmed MSW Salmon\n6. Farmed 1SW Salmon\n"))
    for i in fish_selection:
        if i == '1':
            wild_MSW_number = input("Please enter number of Wild MSW Salmon:")
            wild_MSW_weight = input ("Please enter weight of Wild MSW Salmon:")
        if i == '2':
            wild_1SW_number = input("Please enter number of Wild 1SW Salmon:")
            wild_1SW_weight = input("Please enter weight of Wild 1SW Salmon:")
        if i == '3':
            sea_trout_number = input("Please enter number of sea trout:")
            sea_trout_weight = input("Please enter weight of sea trout:")
        if i == '4':
            finnock_number = input("Please enter number of finnock:")
            finnock_weight = input("Please enter weight of finnock:")
        if i == '5':
            farmed_MSW_number = input("Please enter number of Farmed MSW Salmon:")
            farmed_MSW_weight = input("Please enter weight of Farmed MSW Salmon:")
        if i == '6':
            farmed_1SW_number = input("Please enter number of Farmed 1SW Salmon:")
            farmed_1SW_weight = input("Please enter weight of Farmed 1SW Salmon:") 
    #query = f'INSERT INTO Catches_over_time (district_id, method, year, month_number, wild_MSW_number, wild_MSW_weight, wild_1SW_number, wild_1SW_weigt, sea_trout_number, sea_trout_weight, finnock_number, finnock_weight, farmed_MSW_number, farmed_MSW_weight, farmed_1SW_number, farmed_1SW_weight, netting_effort) VALUES ({district}, {method}, {year}, {month_number}, {wild_MSW_number}, {wild_MSW_weight}, {wild_1SW_number}, {wild_1SW_weight}, {sea_trout_number}, {sea_trout_weight}, {finnock_number}, {finnock_weight}, {farmed_MSW_number}, {farmed_MSW_weight}, {farmed_1SW_number}, {farmed_1SW_weight}, {netting_effort})'
    #query2 = f'INSERT INTO Salmon_and_sea_trout_nets (district, district_id, report_order, region, method, year, month, month_number, wild_MSW_number, wild_MSW_weight, wild_1SW_number, wild_1SW_weigt, sea_trout_number, sea_trout_weight, finnock_number, finnock_weight, farmed_MSW_number, farmed_MSW_weight, farmed_1SW_number, farmed_1SW_weight, netting_effort) VALUES ({district}, {district_id}, {report_order}, {region}, {method}, {year}, {month},{month_number}, {wild_MSW_number}, {wild_MSW_weight}, {wild_1SW_number}, {wild_1SW_weight}, {sea_trout_number}, {sea_trout_weight}, {finnock_number}, {finnock_weight}, {farmed_MSW_number}, {farmed_MSW_weight}, {farmed_1SW_number}, {farmed_1SW_weight}, {netting_effort})'
    try:
        query = '''
        INSERT INTO Catches_over_time (district_id, method, year, month_number, wild_MSW_number, wild_MSW_weight, wild_1SW_number, wild_1SW_weight, sea_trout_number, sea_trout_weight, finnock_number, finnock_weight, farmed_MSW_number, farmed_MSW_weight, farmed_1SW_number, farmed_1SW_weight, netting_effort)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        params = (district_id, method, year, month_number, wild_MSW_number, wild_MSW_weight, wild_1SW_number, wild_1SW_weight, sea_trout_number, sea_trout_weight, finnock_number, finnock_weight, farmed_MSW_number, farmed_MSW_weight, farmed_1SW_number, farmed_1SW_weight, netting_effort)
        cur.execute(query, params)
        query2 = '''
        INSERT INTO Salmon_and_sea_trout_nets (district, district_id, report_order, region, method, year, month, month_number, wild_MSW_number, wild_MSW_weight, wild_1SW_number, wild_1SW_weight, sea_trout_number, sea_trout_weight, finnock_number, finnock_weight, farmed_MSW_number, farmed_MSW_weight, farmed_1SW_number, farmed_1SW_weight, netting_effort)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        params2 = (
            district, district_id, report_order, region, method, year, month, month_number,
            wild_MSW_number, wild_MSW_weight, wild_1SW_number, wild_1SW_weight, 
            sea_trout_number, sea_trout_weight, finnock_number, finnock_weight, 
            farmed_MSW_number, farmed_MSW_weight, farmed_1SW_number, farmed_1SW_weight, netting_effort
        )
        cur.execute(query2, params2)
        conn.commit()
        print("Data inserted succcessfully!")
    except sqlite3.Error as e:
        print('Error:', e)
        conn.rollback()
    return 

def delete_data():
    condition = input('Please enter condition for deletion:')
    query = f'DELETE FROM Catches_over_time WHERE {condition}'
    query2 = f'DELETE FROM Salmon_and_sea_trout_nets WHERE {condition}'
    try:
        cur.execute(query)
        cur.execute(query2)
        conn.commit()
        print('Data deleted successfully!')
    except sqlite3.Error as e:
        print('Error: {}'.format(e))
        conn.rollback()
    return

def update_data():
   table_name = input('Please enter table name:')
   column = input('Please input columun name:')
   new_value = input('Please input your new value:')
   condition = input('Please input your condition:')
   query = f'UPDATE {table_name} SET {column} = {new_value} WHERE {condition}'
   try:
       cur.execute(query)
       conn.commit()
       print("Data updated successfully!")
   except sqlite3.Error as e:
       print('Error: {}'.format(e))
       conn.rollback()
   return

def search_data():
    #table_name = input('Please enter table name:')
    condition = input('Please input condition:')
    query = f'SELECT * FROM Salmon_and_sea_trout_nets WHERE {condition}'
    try:
        cur.execute(query)
        rows = cur.fetchall()
        if not rows:
            print('No result found!\n')
            return
        for i in rows:
            print(i)
    except sqlite3.Error as e:
        print('Error: {}'.format(e))
    return query

def aggregate_data():
    func = input('Please enter the function (e.g., SUM, COUNT, AVG, MAX, MIN:)')
    column = input('Please enter the column:')
    allowed_funcs = {'SUM', 'COUNT', 'AVG', 'MAX', 'MIN'}
    if func not in allowed_funcs:
        print("Invalid function. Please use one of the following:", allowed_funcs)
        return
    choice = input("Do you want to apply an condition? (yes/no): ").lower()
    if choice == 'yes':
        condition = input("Enter the condition ")
        query = f'SELECT {func}({column}) FROM Salmon_and_sea_trout_nets WHERE {condition};'
    else:
        query = f'SELECT {func}({column}) FROM Salmon_and_sea_trout_nets;'
    try: 
        cur.execute(query)
        result = cur.fetchall()
        if not result:
            print('No result found!\n')
            return
        print(result)
    except sqlite3.Error as e:
        print('Error: {}'.format(e))
    return query

def sorting_data():
    column = input('Please enter your columns you want to review:')
    column2 = input('Please enter the column you want to sort:')
    order = 'ASC'
    order_num = input('Please select the order: \n1)Ascending\n2)Descending')
    if order_num == 2:
        order == 'DESC'
    query = f'SELECT {column} FROM Catches_over_time ORDER BY {column2} {order}'
    try:
        cur.execute(query)
        rows = cur.fetchall()
        if not rows:
            print('No result found!\n')
            return
        for i in rows:
            print(i)
    except sqlite3.Error as e:
        print('Error: {}'.format(e))
    return query

def Joining():
    table1 = input("Enter the first table name: ")
    table1_column = input(f"Enter the join column from {table1}: ")
    table2 = input("Enter the second table name: ")
    table2_column = input(f"Enter the join column from {table2}: ")
    output_columns = input("Enter the columns to display (comma-separated, e.g., table1.column1, table2.column2): ")
    query = f'SELECT {output_columns} FROM {table1} INNER JOIN {table2} ON {table1}.{table1_column} = {table2}.{table2_column};'
    try:
        cur.execute(query)
        result = cur.fetchall()
        if not result:
            print('No result found!\n')
            return
        for i in result:
            print(i)
    except sqlite3.Error as e:
        print('Error: {}'.format(e))
    return
    
    

def grouping():
    table = input("Enter the table name: ")
    grouping_columns = input("Enter column(s) to group by (comma-separated if multiple): ")
    choice = input("Do you want to apply an aggregate function? (yes/no): ").lower()
    if choice == 'yes':
        agg_func = input("Enter the aggregate function (e.g., COUNT, SUM, AVG, MAX, MIN): ")
        agg_column = input("Enter the column for the aggregate function: ")
        query = f"SELECT {grouping_columns}, {agg_func}({agg_column}) FROM {table} GROUP BY {grouping_columns}"
    else:
        query = f"SELECT * FROM {table} GROUP BY {grouping_columns}"
    try:
        cur.execute(query)
        result = cur.fetchall()
        if not result:
            print('No result found!\n')
            return
        for i in result:
            print(i)
    except sqlite3.Error as e:
        print('Error: {}'.format(e))
    return

def subqueries(query):
    main_table = input("Enter the main table name: ")
    main_column = input("Enter the column to display from main table: ")
    sub_table = input("Enter the sub-table name: ")
    sub_column = input("Enter the linking column in the sub-table: ")
    main_link_column = input("Enter the linking column in the main table that matches the sub-table:")
    query = f"""SELECT {main_table}.{main_column} FROM {main_table} WHERE {main_table}.{main_link_column} IN (SELECT {sub_table}.{sub_column} FROM {sub_table})"""
    try:    
        result = cur.execute(query)
        for i in result:
            print(i)
    except sqlite3.Error as e:
        print('Error: {}'.format(e))
    return

def main():
    print('Welcome to Salmon Catches Database CLI interface!\n')
    option = cli_interface()
    while option != '10':
        if option == '1':
            insert_data()
        elif option == '2':
            delete_data()
        elif option == '3':
            update_data()
        elif option == '4':
            search_data()
        elif option == '5':
            aggregate_data()
        elif option == '6':
            sorting_data()
        elif option == '7':
            Joining()
        elif option == '8':
            grouping()
        elif option == '9':
            subqueries()
        else:
            print('Invalid input!')
        option = cli_interface()
    return 


conn = sqlite3.connect('SalmonCatchDatabase.db')
cur = conn.cursor()
c = main()
conn.commit()
cur.close()
conn.close()