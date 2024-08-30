import json
import os
import random
import threading

from Milestone3DB import Milestone3DB

# -------------------------------- Emoticons --------------------------------------------
emoticons = ['ಥ_ಥ', '(╬ ಠ益ಠ)', 'ლ(｀ー´ლ)', '(╯°□°）╯︵ ┻━┻', '༼∵༽ ༼⍨༽ ༼⍢༽ ༼⍤༽', '༼ ༎ຶ ෴ ༎ຶ༽', '{ಠʖಠ}']

class ParseJSON:
    def __init__(self, num_threads):
        self.num_threads = num_threads if num_threads is not None else os.cpu_count() - 2
        self.lock = threading.Lock()
        self.iteration = 0
        self.item_count = 0
        self.barrier = threading.Barrier(self.num_threads)
        self.emoticon = ''
        print('Using %d threads' % self.num_threads)

    # ------------------------- Helper Methods ------------------------------------------------------
    def update_emoticon(self):
        self.emoticon = random.choice(emoticons)

    def thread_safe_increment(self, message, thread_idx=0):
        if thread_idx == 0:
            self.loading_bar(message)
        with self.lock:
            self.iteration += 1

    def loading_bar(self, message, length=100):
        static_item_count = self.item_count
        static_iteration = self.iteration if (100 * self.iteration / float(
            static_item_count)) < 99 else static_item_count
        percent = ("{0:.2f}").format(100 * (static_iteration / float(static_item_count)))
        filled_length = int(length * static_iteration // static_item_count)
        bar = '=' * filled_length + ' ' + self.emoticon + ' =>' + ' ' * (length - filled_length - 1)

        message_length = len(message)
        message_position = (length - message_length) // 2
        bar_with_message = bar[:message_position] + message + bar[message_position:]
        print(f'\r{bar_with_message}|{percent}%', end='\r', flush=True)

    def clean_str_4_sql(self, s):
        return s.replace("'", "''").replace("\n", " ")

    def get_attributes(self, attributes):
        L = []
        for (attribute, value) in list(attributes.items()):
            if isinstance(value, dict):
                L += self.get_attributes(value)
            else:
                L.append((attribute, value))
        return L

    def json_to_memory(self, file_path, print_statement):
        print(f'\nParsing {print_statement}...\n')
        data = []
        count_line = 0
        with open(file_path, 'r') as f:
            for line in f:
                data.append(json.loads(line))
                count_line += 1
            f.close()
        print(f'{count_line} {print_statement} in memory.')
        return data

    # ---------------------------------- Run Threads -----------------------------------------------------------------
    def run_threads(self):
        db = Milestone3DB('localhost', 'milestone3db', 'postgres', '', 5432)
        db.create_connection_pool(self.num_threads)
        self.update_emoticon()
        data = self.json_to_memory('../yelpInput/yelp_business.JSON', 'businesses')
        self.start_threads(self.parse_business_data, data, db)
        self.update_emoticon()
        data = self.json_to_memory('../yelpInput/yelp_user.JSON', 'users')
        self.start_threads(self.parse_user_data, data, db)
        self.update_emoticon()
        self.iteration = 0
        data = self.json_to_memory('../yelpInput/yelp_review.JSON', 'reviews')
        self.start_threads(self.parse_review_data, data, db)
        self.update_emoticon()
        data = self.json_to_memory('../yelpInput/yelp_checkin.JSON', 'checkins')
        self.start_threads(self.parse_checkin_data, data, db)
        print(f'Parsed {len(data)} checkins')

    def start_threads(self, function, items, db):
        self.iteration = 0
        if self.num_threads < 2:
            self.num_threads = 2
        chunks = self.get_chunks(items)
        threads = []
        for i in range(self.num_threads):
            thread = threading.Thread(target=function, args=(i, chunks[i], db))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        print()

    def get_chunks(self, items):
        # // Is the floor division operator. We need this to avoid indices that are out of bounds.
        # Set the item count as a chunk, we'll count with one thread.
        self.item_count = len(items)
        chunk_size = chunk_size = len(items) // self.num_threads
        # Create individual chunks inside the chunks list appending a total of chunk_size items to the sub-list.
        chunks = [items[i * chunk_size:(i + 1) * chunk_size] for i in range(self.num_threads)]
        # Get the leftover items - items[self.num_threads * chunk_size:]
        leftover = items[self.num_threads * chunk_size:]
        # Place the remainders in the chunks.
        for i, item in enumerate(leftover):
            chunks[i].append(item)

        return chunks

    # ---------------------------------- Parse Data -----------------------------------------------------------------
    def parse_business_data(self, thread_idx, data, db):
        connection = db.get_connection()
        business_batch = []
        category_batch = []
        business_category_batch = []
        business_hours_batch = []
        try:
            for i in range(len(data)):
                datum = data[i]
                business = datum['business_id']  # business id
                business_batch.append({
                    'business_id': self.clean_str_4_sql(business),
                    'name': self.clean_str_4_sql(datum['name']),
                    'address': self.clean_str_4_sql(datum['address']),
                    'city': self.clean_str_4_sql(datum['city']),
                    'state': self.clean_str_4_sql(datum['state']),
                    'fk_zipcode': datum['postal_code'],
                    'latitude': datum['latitude'],
                    'longitude': datum['longitude'],
                    'stars': 0,
                    'total_stars': 0,
                    'review_count': 0,
                    'num_checkins': 0
                })

                # process business categories
                for category in datum['categories']:
                    category_batch.append({
                        'category': self.clean_str_4_sql(category),
                    })
                    business_category_batch.append({
                        'fk_business_id': self.clean_str_4_sql(business),
                        'fk_category': self.clean_str_4_sql(category),
                    })

                # process business hours
                for (day, hours) in datum['hours'].items():
                    business_hours_batch.append({
                        'fk_business_id': self.clean_str_4_sql(business),
                        'day_of_week': self.clean_str_4_sql(day),
                        'hours': self.clean_str_4_sql(hours)
                    })

                message = 'ADDING BUSINESSES'
                self.thread_safe_increment(message, thread_idx)

            with self.lock:
                db.insert_batch(connection, 'business', business_batch, conflict_columns=['business_id'])
            self.barrier.wait()
            with self.lock:
                db.insert_batch(connection, 'categories', category_batch, conflict_columns=['category'])
            self.barrier.wait()
            with self.lock:
                db.insert_batch(connection, 'business_categories', business_category_batch,
                                conflict_columns=['fk_business_id', 'fk_category'])
            self.barrier.wait()
            with self.lock:
                db.insert_batch(connection, 'business_hours', business_hours_batch,
                                conflict_columns=['fk_business_id', 'day_of_week'])
        finally:
            self.barrier.wait()
            db.release_connection(connection)

    def parse_review_data(self, thread_idx, data, db):
        connection = db.get_connection()
        review_batch = []
        for i in range(len(data)):
            datum = data[i]
            review_batch.append({
                'review_id': self.clean_str_4_sql(datum['review_id']),
                'fk_user_id': self.clean_str_4_sql(datum['user_id']),
                'fk_business_id': self.clean_str_4_sql(datum['business_id']),
                'stars': datum['stars'],
                'date': datum['date'],
                'text': self.clean_str_4_sql(datum['text']),
                'useful': (datum['useful']),
                'funny': (datum['funny']),
                'cool': (datum['cool']),
            })
            message = 'ADDING REVIEWS'
            self.thread_safe_increment(message, thread_idx)

        with self.lock:
            db.insert_batch(connection, 'review', review_batch, conflict_columns=['review_id'])

        db.release_connection(connection)

    def parse_user_data(self, thread_idx, data, db):
        connection = db.get_connection()
        user_batch = []
        for i in range(len(data)):
            datum = data[i]
            user_batch.append({
                'user_id': self.clean_str_4_sql(datum['user_id']),
                'name': self.clean_str_4_sql(datum['name']),
                'yelping_since': self.clean_str_4_sql(datum['yelping_since']),
                'review_count': datum['review_count'],
                'fans': datum['fans'],
                'average_stars': datum['average_stars'],
                'funny': datum['funny'],
                'useful': datum['useful'],
                'cool': datum['cool'],
            })
            message = 'ADDING USERS'
            self.thread_safe_increment(message, thread_idx)
        with self.lock:
            db.insert_batch(connection, 'yelp_user', user_batch, conflict_columns=['user_id'])

        if thread_idx == 0:
            print()
            print('Parsing friends...')
            self.iteration = 0

        self.barrier.wait()

        friend_batch = []
        for i in range(len(data)):
            datum = data[i]
            user_id = datum['user_id']
            for friend in datum['friends']:
                friend_batch.append({
                    'fk_user_id': self.clean_str_4_sql(user_id),
                    'fk_friend_id': self.clean_str_4_sql(friend),
                })

            message = 'ADDING FRIENDS'
            self.thread_safe_increment(message, thread_idx)

        with self.lock:
            db.insert_batch(connection, 'friend', friend_batch, conflict_columns=['fk_friend_id', 'fk_user_id'])

        db.release_connection(connection)

    def parse_checkin_data(self, thread_idx, data, db):
        connection = db.get_connection()
        checkin_day_batch = []
        checkin_hour_batch = []
        for i in range(len(data)):
            datum = data[i]
            business_id = self.clean_str_4_sql(datum['business_id'])

            for day, hours in datum['time'].items():
                day_of_week = self.clean_str_4_sql(day)
                checkin_day_batch.append({
                    'day': day_of_week,
                    'fk_business_id': business_id,
                })

            message = 'ADDING CHECKIN DAYS'
            self.loading_bar(message, thread_idx)
            self.thread_safe_increment(message, thread_idx)

        if thread_idx == 0:
            self.iteration = 0

        with self.lock:
            db.insert_batch(connection, 'checkin_day', checkin_day_batch, conflict_columns=['fk_business_id', 'day'])
        checkin_day_batch.clear()
        self.barrier.wait()

        for i in range(len(data)):
            datum = data[i]
            business_id = self.clean_str_4_sql(datum['business_id'])
            for day, times in datum['time'].items():
                day = self.clean_str_4_sql(day)
                checkin_day_id = db.get_checkin_day_fk(connection, day, business_id)
                for hour, count in times.items():
                    checkin_hour_batch.append({
                        'fk_day_id': checkin_day_id,
                        'total_checkins': count,
                        'hour': self.clean_str_4_sql(hour)
                    })

            message = 'ADDING CHECKIN-HOURS'
            self.thread_safe_increment(message, thread_idx)

        with self.lock:
            db.insert_batch(connection, 'checkin_hour', checkin_hour_batch, conflict_columns=['time_id'])

        db.release_connection(connection)


if __name__ == '__main__':
    pj = ParseJSON(None)
    pj.run_threads()
