import sys
import psycopg2
from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QApplication, QWidget, QComboBox, QLabel, QListWidget, QMessageBox, \
    QGridLayout, QFrame, QVBoxLayout, QTableWidget, QTableWidgetItem, QHeaderView, QPushButton
from PyQt6.uic.properties import QtWidgets


class MilestoneApp(QWidget):
    def __init__(self):
        super().__init__()
        self.conn = None
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle('Businesses - Milestone 3')

        grid_layout = QGridLayout()

        # Add the title label and make it look official.
        self.title_label = QLabel('Businesses - Milestone 3')
        self.title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.title_label.setStyleSheet("""
                    QLabel {
                        font-size: 24px;
                        font-weight: bold;
                        font-family: mono;
                        color: #333333;
                        padding: 10px;
                        margin: 10px;
                    }
                """)
        grid_layout.addWidget(self.title_label, 0, 0)

        # Add the distinct states.
        self.distinct_states_label = QLabel('State')
        self.distinct_states_combo = QComboBox()

        # Add the cities.
        self.cities_label = QLabel('Cities: ')
        self.cities_list = QListWidget()

        # Add zipcodes
        self.zipcode_label = QLabel('Zipcodes: ')
        self.zipcode_list = QListWidget()

        # Add categories table
        self.categories_label = QLabel('Categories: ')
        self.categories_list = QListWidget()

        # Add zip labels
        self.zip_statistics_label = QLabel('Zip Statistics: ')
        self.zip_label = QLabel('Zipcode: N/A')
        self.zip_population = QLabel('Population: N/A')
        self.zip_median = QLabel('Median Salary: N/A')
        self.zip_business_count = QLabel('Number of Businesses: N/A')

        grid_layout.addWidget(self.set_location(), 1, 0)
        grid_layout.addWidget(self.zipcode_statistics(), 1, 1)

        # Add Popular businesses
        self.popular_label = QLabel('Popular: ')
        self.popular_list = QTableWidget()

        # Add Successful businesses
        self.successful_label = QLabel('Successful: ')
        self.successful_list = QTableWidget()

        grid_layout.addWidget(self.set_business_lists(), 2, 0, 1, 2)

        self.refresh_classification_button = QPushButton('Refresh Popular/Successful')
        grid_layout.addWidget(self.refresh_classification_button, 3, 1)

        self.setLayout(grid_layout)

        # Connect to the database.
        self.connect_db()

        # Update components to populate with values.
        self.update_state_combo()
        self.update_city_list()

        # Register listeners to the states combobox and the cities list.
        self.distinct_states_combo.currentIndexChanged.connect(self.update_city_list)
        self.cities_list.itemClicked.connect(self.update_zipcode_list)
        self.zipcode_list.itemClicked.connect(self.update_categories_list)
        self.categories_list.itemClicked.connect(self.update_business_table)
        self.refresh_classification_button.clicked.connect(self.update_successful)
        self.refresh_classification_button.clicked.connect(self.update_popular)

    def clear_business_table(self):
        self.business_table.clearContents()

    def add_classifier_tables(self, classifier_label, classifier_table):
        classifier_table.setColumnCount(3)
        for i in range(3):
            classifier_table.setColumnWidth(i, 200)

        classifier_table.horizontalHeader().setVisible(False)
        classifier_table.verticalHeader().setVisible(False)

        return self.container(classifier_label, classifier_table)

    def add_business_table(self):
        # Add the business table.
        self.business_label = QLabel('Business: ')
        self.business_table = QTableWidget()

        # Set number of columns in the table.
        self.business_table.setColumnCount(6)

        # Set the width of the city and state column.
        for i in range(6):
            self.business_table.setColumnWidth(i, 200)

        # Stretch the business name column to accommodate the space on the page.
        business_header = self.business_table.horizontalHeader()
        business_header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)

        # We don't want to see row numbers.
        self.business_table.verticalHeader().setVisible(False)
        self.business_table.horizontalHeader().setVisible(False)

        # Create a new container for this item.
        return self.container(self.business_label, self.business_table)

    # Connect to the PostgreSQL database.
    def connect_db(self):
        try:
            self.conn = psycopg2.connect(
                host='localhost',
                dbname='milestone3db',
                user='postgres',
                password='',
                port=5432
            )
        except Exception as e:
            QMessageBox.critical(self, 'Could not connect to the database:', str(e))

    def update_state_combo(self):
        if self.conn is None:
            QMessageBox.warning(self, 'Warning', "Could not connect to the database.")

        try:
            # Execute the command: SELECT DISTINCT state FROM business;
            cursor = self.conn.cursor()
            cursor.execute('select distinct state from business;')
            distinct_states = cursor.fetchall()
            cursor.close()

            # Populate the state combo box.
            for state in distinct_states:
                self.distinct_states_combo.addItems(state)

            self.cities_list.clear()
            self.clear_business_table()
            self.clear_zipcode_statistics()

        except Exception as e:
            QMessageBox.warning(self, 'Warning:', str(e))

    def update_city_list(self):
        # Clear the list of cities every time a state is changed.
        self.cities_list.clear()

        if self.conn is None:
            QMessageBox.warning(self, 'Warning', "Could not connect to the database.")

        # Get the current state selected.
        current_state = self.distinct_states_combo.currentText()

        try:
            cursor = self.conn.cursor()

            # Execute the command: SELECT DISTINCT city FROM business WHERE state=[selected state] ORDER BY city
            cursor.execute('select distinct city from business where state=%s order by city;', (current_state,))

            # Get the cities from the cursor.
            cities = cursor.fetchall()
            cursor.close()

            # Update the cities in the list.
            for city in cities:
                self.cities_list.addItems(city)

            self.categories_list.clear()
            self.clear_business_table()
            self.clear_zipcode_statistics()
            self.zipcode_list.clear()

        except Exception as e:
            QMessageBox.warning(self, 'Warning:', str(e))

    def update_zipcode_list(self):
        self.clear_zipcode_statistics()
        self.zipcode_list.clear()

        if self.conn is None:
            QMessageBox().warning(self, 'Warning', "Could not connect to the database.")

        current_state = self.distinct_states_combo.currentText()
        current_city = self.cities_list.currentItem().text()

        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    select distinct fk_zipcode from business
                    where state = %s and city = %s
                    order by fk_zipcode
            """, (current_state, current_city))

                zipcodes = cursor.fetchall()

                for zipcode in zipcodes:
                    self.zipcode_list.addItems(zipcode)

                self.categories_list.clear()
                self.clear_business_table()
                cursor.close()
        except Exception as e:
            QMessageBox.warning(self, 'Warning:', str(e))

    def update_categories_list(self):
        self.categories_list.clear()
        self.update_zipcode_statistics()
        current_state = self.distinct_states_combo.currentText()
        current_city = self.cities_list.currentItem().text()
        current_zip = self.zipcode_list.currentItem().text()

        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    select distinct bc.fk_category
                    from business b
                    inner join business_categories bc 
                    on b.business_id = bc.fk_business_id
                    where b.state = %s and b.city = %s and b.fk_zipcode = %s 
                    order by bc.fk_category
                """, (current_state, current_city, current_zip))

                categories = cursor.fetchall()

                for category in categories:
                    self.categories_list.addItem(category[0])

                cursor.close()
                self.clear_business_table()

        except Exception as e:
            QMessageBox.warning(self, 'Warning:', str(e))

    def update_business_table(self):
        # We want to set the row count to zero as the clear function doesn't clear all the rows from the table.
        self.clear_business_table()
        self.business_table.setRowCount(0)

        if self.conn is None:
            QMessageBox.warning(self, 'Warning', "Could not connect to the database.")

        # Get the current city and state.
        current_state = self.distinct_states_combo.currentText()
        current_zip = self.zipcode_list.currentItem().text()
        current_category = self.categories_list.currentItem().text()

        try:
            businesses = []
            cursor = self.conn.cursor()
            # Execute the command: SELECT name, state, city FROM business WHERE state=[select state] AND city=[select city] ORDER BY name;
            cursor.execute("""
                select name, address, city, stars, review_count, num_checkins
                from business b
                inner join business_categories bc
                on b.business_id = bc.fk_business_id
                where state = %s and fk_zipcode = %s and bc.fk_category = %s
                order by (name)
            """, (current_state, current_zip, current_category))
            businesses = cursor.fetchall()

            # There are a lot of values being updated here. It follows we don't want to display all these changes.
            self.business_table.setUpdatesEnabled(False)
            for business in businesses:
                self.add_business(self.business_table, business)
            # Re-enable updates in the business table to show the changes.
            self.business_table.setUpdatesEnabled(True)

        except Exception as e:
            QMessageBox.warning(self, 'Warning:', str(e))

    def add_business(self, table, business):
        # The sql command returns a list of tuples, that is, (name,state,city).
        name, address, city, stars, review_count, num_checkins = business
        stars = str(stars)
        num_checkins = str(num_checkins)
        review_count = str(review_count)
        current_row = table.rowCount()
        # Create a new row at the last index and insert appropriate values.
        table.insertRow(current_row)
        table.setItem(current_row, 0, QTableWidgetItem(name))
        table.setItem(current_row, 1, QTableWidgetItem(address))
        table.setItem(current_row, 2, QTableWidgetItem(city))
        table.setItem(current_row, 3, QTableWidgetItem("Stars: " + stars))
        table.setItem(current_row, 4, QTableWidgetItem("Reviews: " + review_count))
        table.setItem(current_row, 5, QTableWidgetItem("Total Check-ins: " + num_checkins))

    def clear_zipcode_statistics(self):
        self.zip_label.clear()
        self.zip_median.clear()
        self.zip_population.clear()
        self.zip_business_count.clear()
        self.zip_label.setText('Zipcode: N/A')
        self.zip_population.setText('Population: N/A')
        self.zip_median.setText('Median: N/A')
        self.zip_business_count.setText('Business Count: N/A')

    def update_zipcode_statistics(self):
        if self.conn is None:
            QMessageBox.warning(self, 'Warning', "Could not connect to the database.")

        current_zip = self.zipcode_list.currentItem().text()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    select median_income, population
                    from zipcode where zipcode = %s
                """, (current_zip,))
                zip_statistics = cursor.fetchall()
                if zip_statistics is None:
                    return

                self.zip_label.clear()
                self.zip_median.clear()
                self.zip_population.clear()
                median_income, population = zip_statistics[0]
                self.zip_label.setText(f'Zipcode: {current_zip}')
                self.zip_population.setText(f'Population: {population}')
                self.zip_median.setText(f'Median Income: ${median_income}')

                cursor.execute("""
                    select count(*) from business b 
                    where fk_zipcode = %s
                """, (current_zip,))
                self.zip_business_count.setText(f'Business Count: {cursor.fetchall()[0][0]}')
        except Exception as e:
            QMessageBox.warning(self, 'Warning:', str(e))

    def update_popular(self):
        zipcode = self.zipcode_list.currentItem().text()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    with partition as (
                        select
                            name,
                            stars,
                            review_count,
                            business_id,
                            num_checkins,
                            fk_zipcode,
                            city,
                            row_number() over (partition by fk_zipcode order by num_checkins desc) as rn
                        from business
                        where fk_zipcode = %s
                    )
                    select name, stars, review_count
                    from partition
                    where rn <= 10
                """, (zipcode,))
                popular = cursor.fetchall()
                self.popular_list.setRowCount(0)
                self.popular_list.clear()
                for pop in popular:
                    self.add_to_classification_tables(self.popular_list, pop)

        except Exception as e:
            QMessageBox.warning(self, 'Warning:', str(e))

    def update_successful(self):
        zipcode = self.zipcode_list.currentItem().text()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    with partitioned_vals as (
                    select
                        fk_zipcode,
                        business_id,
                        num_checkins,
                        stars,
                        row_number() over (partition by fk_zipcode order by num_checkins desc) as rn,
                        count(*) over (partition by fk_zipcode) as business_count
                    from business
                ),
                median_half as (
                    select
                        fk_zipcode,
                        business_id,
                        num_checkins,
                        stars,
                        rn,
                        count(*) over (partition by fk_zipcode) as business_count
                    from partitioned_vals pv
                    where rn <= (business_count + 1) / 2
                )
                select
                    b.name,
                    b.stars,
                    b.review_count
                from business b
                left join median_half mh on b.business_id = mh.business_id
                where b.fk_zipcode = %s
                and mh.stars >= 4.0
                and mh.business_id is not null
                order by b.stars desc;
                """, (zipcode, ))

                successful = cursor.fetchall()
                self.successful_list.setRowCount(0)
                self.successful_list.clear()
                for success in successful:
                    self.add_to_classification_tables(self.successful_list, success)

        except Exception as e:
            QMessageBox.warning(self, 'Warning:', str(e))

    def add_to_classification_tables(self, table, business):
        name, stars, review_count = business
        stars = str(stars)
        review_count = str(review_count)
        current_row = table.rowCount()
        # Create a new row at the last index and insert appropriate values.
        table.insertRow(current_row)
        table.setItem(current_row, 0, QTableWidgetItem(name))
        table.setItem(current_row, 1, QTableWidgetItem("Stars: " + stars))
        table.setItem(current_row, 2, QTableWidgetItem("Reviews: " + review_count))
    def set_zipcode_statistics(self):
        layout = QGridLayout()
        layout.addWidget(self.zip_label, 0, 0)
        layout.addWidget(self.zip_population, 1, 0)
        layout.addWidget(self.zip_median, 2, 0)
        layout.addWidget(self.zip_business_count, 3, 0)
        bg = QFrame()
        bg.setLayout(layout)
        return bg

    def zipcode_statistics(self):
        layout = QGridLayout()
        layout.addWidget(self.set_zipcode_statistics(), 0, 0)
        layout.addWidget(self.categories_label, 1, 0)
        layout.addWidget(self.categories_list, 2, 0, 1, 2)
        bg = QFrame(self)
        bg.setStyleSheet(f"""
            background-color: #f0f0f0;
                    border: 1px solid #d1d1d1;
                    border-radius: 1px;   
        """)
        bg.setLayout(layout)
        return bg

    def set_location(self):
        layout = QGridLayout()
        layout.addWidget(self.distinct_states_label, 0, 0)
        layout.addWidget(self.distinct_states_combo, 0, 1)
        layout.addWidget(self.cities_label, 1, 0)
        layout.addWidget(self.cities_list, 2, 0)
        layout.addWidget(self.zipcode_label, 1, 1)
        layout.addWidget(self.zipcode_list, 2, 1)
        bg = QFrame(self)
        bg.setStyleSheet(f"""
            background-color: #f0f0f0;
                    border: 1px solid #d1d1d1;
                    border-radius: 1px;   
        """)
        bg.setLayout(layout)
        return bg

    def set_business_lists(self):
        layout = QGridLayout()
        layout.addWidget(self.add_business_table(), 0, 0, 1, 2)
        popular_widget = self.add_classifier_tables(self.popular_label, self.popular_list)
        layout.addWidget(popular_widget, 1, 0)
        successful_widget = self.add_classifier_tables(self.successful_label, self.successful_list)
        layout.addWidget(successful_widget, 1, 1)
        bg = QFrame(self)
        bg.setLayout(layout)
        bg.setStyleSheet(f"""
             background-color: #f0f0f0;
                    border: 1px solid #d1d1d1;
                    border-radius: 1px;       
        """)
        return bg

    # Create styled containers.
    def container(self, component_label, component):
        bg = QFrame(self)

        # Add Style
        bg.setStyleSheet(f"""
            QFrame {{
                background-color: #f0f0f0 ;
                border: 1px solid #d1d1d1;
                border-radius: 1px;      
                padding: 6px;           
                margin: 6px;           
            }}
            
            QLabel {{
                font-size: 16px;
                color: #333333;
            }}
            
            QListWidget {{
                border-radius: 1px;
                border: 1px solid #ccc;
                padding: 5px;
                background-color: white;
            }}
            
            
        """)

        layout = QVBoxLayout()
        if component_label is not None:
            layout.addWidget(component_label)

        if component is not None:
            layout.addWidget(component)

        bg.setLayout(layout)

        return bg


if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = MilestoneApp()
    ex.show()
    sys.exit(app.exec())
