import math
import numpy as np
import pandas as pd
import geopandas as gpd
import psycopg2
import neo4j
import statistics
from geographiclib.geodesic import Geodesic
from IPython.display import display
import os

class StationManager():
    def __init__(self, weight):
        self.weight = weight
        self.connect()

    def connect(self):
        self.driver = neo4j.GraphDatabase.driver(uri="neo4j://neo4j:7687", auth=("neo4j","w205"))
        self.session = self.driver.session(database="neo4j")

        self.connection = psycopg2.connect(
            user = "postgres",
            password = "ucb",
            host = "postgres",
            port = "5432",
            database = "postgres"
        )

        self.cursor = self.connection.cursor()

    def clear_graph(self):
        query = "CALL gds.graph.drop($graph, false)"
        self.session.run(query, graph='ds_graph')

    def reset_graph(self):
        query = "match (node)-[relationship]->() delete node, relationship"
        self.session.run(query)

        query = "match (node) delete node"
        self.session.run(query)

        self.clear_graph()
        self.drop_all_tables()
        self.create_all_tables()
        self.load_csv_files_into_tables()

        return  self.number_nodes_relationships()

    def load_csv_files_into_tables(self):
        self.connection.rollback()
        cwd = os.getcwd()
        query = """
    
        copy stations
        from '{cwd}/stations.csv' delimiter ',' NULL '' csv header;
    
        """.format(cwd=cwd)
    
        self.cursor.execute(query)
    
        self.connection.commit()
        self.connection.rollback()
    
        query = """
    
        copy lines
        from '{cwd}/lines.csv' delimiter ',' NULL '' csv header;
    
        """.format(cwd=cwd)
    
        self.cursor.execute(query)
    
        self.connection.commit()
        self.connection.rollback()
    
        query = """
    
        copy travel_times
        from '{cwd}/travel_times.csv' delimiter ',' NULL '' csv header;
    
        """.format(cwd=cwd)
    
        self.cursor.execute(query)
    
        self.connection.commit()
    
    
    def read_csv_file(self, file_name, limit):
        "read the csv file and print only the first limit rows"
    
        csv_file = open(file_name, "r")
    
        csv_data = csv.reader(csv_file)
    
        i = 0
    
        for row in csv_data:
            i += 1
            if i <= limit:
                print(row)
    
        print("\nPrinted ", min(limit, i), "lines of ", i, "total lines.")


    def get_zipcode_population_by_station(self, miles=0.2, order_by='total_population', ascending=False):

        zips = []
        populations = []
        total_populations = []
        stations = []
        for station in self.get_stations()['station']:
            z, p, t = self.station_get_zips(station, miles)
            zips += [z]
            populations += [p]
            total_populations += [t]
            stations += [station]

        summed_populations = []
        for tp in total_populations:
            population_sum = 0
            for p in tp:
                population_sum = int(population_sum + p)
            summed_populations += [population_sum]
        df = pd.DataFrame(stations, columns=['station'])
        df['zipcode'] = zips
        df['population'] = populations
        df['total_population'] = summed_populations
        df.sort_values(by=order_by, ascending=ascending, inplace=True)
        return df


    def get_zipcodes(self):
        "return DataFrame for all stations"
        rollback_before_flag = True
        rollback_after_flag = True
    
        query = """
    
        select *
        from zip_codes
    
        """
    
        return self.run_query_pandas(query)


    def get_stations(self):
        "return DataFrame for all stations"
        rollback_before_flag = True
        rollback_after_flag = True
    
        query = """
    
        select *
        from stations
        order by station
    
        """
    
        return self.run_query_pandas(query)


    def get_customers(self):
        "return DataFrame for all customers"
        rollback_before_flag = True
        rollback_after_flag = True
    
        query = """
    
        select *
        from customers
    
        """
    
        return self.run_query_pandas(query)


    def get_lines(self):
        "return DataFrame of all lines"
        rollback_before_flag = True
        rollback_after_flag = True
    
        query = """
    
        select *
        from lines
        order by line, sequence
    
        """
    
        return self.run_query_pandas(query)


    def get_travel_times(self):
        "return DataFrame of all travel times"
        rollback_before_flag = True
        rollback_after_flag = True
    
        query = """
    
        select *
        from travel_times
        order by station_1, station_2
    
        """
    
        return self.run_query_pandas(query)

    def drop_all_tables(self):
        self.connection.rollback()
    
        query = """
    
        drop table if exists stations;
    
        """
    
        self.cursor.execute(query)
    
        self.connection.commit()
        self.connection.rollback()
    
        query = """
    
        drop table if exists lines;
    
        """
    
        self.cursor.execute(query)
    
        self.connection.commit()
        self.connection.rollback()
    
        query = """
    
        drop table if exists travel_times;
    
        """
    
        self.cursor.execute(query)
    
        self.connection.commit()
    
    
    def create_all_tables(self):
        self.connection.rollback()
    
        query = """
    
        create table stations (
            station varchar(32),
            latitude numeric(9,6),
            longitude numeric(9,6),
            transfer_time numeric(3),
            primary key (station)
            );
    
        """
    
        self.cursor.execute(query)
    
        self.connection.commit()
    
        self.connection.rollback()
    
        query = """
    
        create table lines (
            line varchar(6),
            sequence numeric(2),
            station varchar(32),
            primary key (line, sequence)
            );
    
        """
    
        self.cursor.execute(query)
    
        self.connection.commit()
    
        query = """
    
        create table travel_times (
            station_1 varchar(32),
            station_2 varchar(32),
            travel_time numeric(3),
            primary key (station_1, station_2)
        );
    
        """
    
        self.cursor.execute(query)
    
        self.connection.commit()


    def create_dag(self):
        "create the neo4j graph by creating the station nodes, line nodes and relationships"

        self.reset_graph()
        self.create_station_nodes()
        self.create_one_way_station_relationships()

        return self.number_nodes_relationships()


    def create_graph(self):
        "create the neo4j graph by creating the station nodes, line nodes and relationships"

        self.reset_graph()
        self.create_one_way_station_nodes()
        self.create_line_nodes_and_relationships()
        self.create_transfer_relationships()
        self.create_two_way_station_relationships()

        return self.number_nodes_relationships()

    def create_station_nodes(self):
        "create nodes in neo4j graph"

        self.connection.rollback()

        query = """
        
        select station
        from stations
        order by station
        
        """
        
        self.cursor.execute(query)
        
        self.connection.rollback()
        
        rows = self.cursor.fetchall()
        
        for row in rows:
            station = row[0]
            self.create_node(station)


    def create_one_way_station_nodes(self):
        "create nodes and one-way relationships in neo4j graph"

        self.connection.rollback()

        query = """
        
        select station
        from stations
        order by station
        
        """
        
        self.cursor.execute(query)
        
        self.connection.rollback()
        
        rows = self.cursor.fetchall()
        
        for row in rows:
            
            station = row[0]
            depart = 'depart ' + station
            arrive = 'arrive ' + station
    
            self.create_node('depart ' + station)
            self.create_node('arrive ' + station)


    def create_line_nodes_and_relationships(self):
        "create nodes and one-way relationships in neo4j graph"

        self.connection.rollback()

        query = """
        
        select station, line
        from lines
        order by station, line
        
        """
        
        self.cursor.execute(query)
        
        self.connection.rollback()
        
        rows = self.cursor.fetchall()
        
        for row in rows:
            
            station = row[0]
            line = row[1]
            depart = 'depart ' + station
            arrive = 'arrive ' + station
            line_station = line + ' ' + station
    
            self.create_node(line_station)
            self.create_relationship_one_way(depart, line_station, 0)
            self.create_relationship_one_way(line_station, arrive, 0)


    def create_transfer_relationships(self):
        "create line transfer relationships with time as the weight on the line transfer links"

        self.connection.rollback()

        query = """
        
        select a.station, a.line as from_line, b.line as to_line, s.transfer_time
        from lines a
             join lines b
               on a.station = b.station and a.line <> b.line
             join stations s
               on a.station = s.station
        order by 1, 2, 3
        
        """
        
        self.cursor.execute(query)
        
        self.connection.rollback()
        
        rows = self.cursor.fetchall()
        
        for row in rows:
        
            station = row[0]
            from_line = row[1]
            to_line = row[2]
            transfer_time = int(row[3])
        
            from_station = from_line + ' ' + station
            to_station = to_line + ' ' + station
        
            self.create_relationship_one_way(from_station, to_station, transfer_time)


    def create_one_way_station_relationships(self):
        "create 1-way relationships from each station"

        self.connection.rollback()
        
        query = """
        select a.line, a.station as from_station, b.station as to_station, t.travel_time
        from lines a
          join lines b
            on a.line = b.line and b.sequence = (a.sequence + 1)
          join travel_times t
            on (a.station = t.station_1 and b.station = t.station_2)
                or (a.station = t.station_2 and b.station = t.station_1)
        order by line, from_station, to_station
        """

        self.cursor.execute(query)
        
        self.connection.rollback()
        
        rows = self.cursor.fetchall()
        
        for row in rows:
            
            line = row[0]
            from_station = row[1]
            to_station = row[2]
            travel_time = int(row[3])
            
            self.create_relationship_one_way(from_station=from_station, to_station=to_station, weight=travel_time)


    def create_two_way_station_relationships(self):
        "create 2-way relationships from each station"

        self.connection.rollback()
        
        query = """
        select a.line, a.station as from_station, b.station as to_station, t.travel_time
        from lines a
          join lines b
            on a.line = b.line and b.sequence = (a.sequence + 1)
          join travel_times t
            on (a.station = t.station_1 and b.station = t.station_2)
                or (a.station = t.station_2 and b.station = t.station_1)
        order by line, from_station, to_station
        """

        self.cursor.execute(query)
        
        self.connection.rollback()
        
        rows = self.cursor.fetchall()
        
        for row in rows:
        
            line = row[0]
            from_station = line + ' ' + row[1]
            to_station = line + ' ' + row[2]
            travel_time = int(row[3])
        
            self.create_relationship_two_way(from_station, to_station, travel_time)


    def run_query_pandas(self, query, rollback_before_flag=True, rollback_after_flag=True):
        "function to run a select query and return rows in a pandas dataframe"
        
        if rollback_before_flag:
            self.connection.rollback()
        
        df = pd.read_sql_query(query, self.connection)
        
        if rollback_after_flag:
            self.connection.rollback()
        
        # fix the float columns that really should be integers
        
        for column in df:
        
            if df[column].dtype == "float64":
    
                fraction_flag = False
    
                for value in df[column].values:
                    
                    if not np.isnan(value):
                        if value - math.floor(value) != 0:
                            fraction_flag = True
    
                if not fraction_flag:
                    df[column] = df[column].astype('Int64')
        
        return(df)
    

    def run_query_neo4j(self, query, **kwargs):
        "run a neo4j related query and return the results in a pandas dataframe"
        
        result = self.session.run(query, **kwargs)
        
        df = pd.DataFrame([r.values() for r in result], columns=result.keys())
        
        return df     


    def create_node(self, station_name):
        "create a node with label Station"
    
        query = """
    
        CREATE (:Station {name: $station_name})
    
        """
    
        self.session.run(query, station_name=station_name)
    
    
    def number_nodes_relationships(self):
        "return a DataFrame holding the number of nodes, relationsips and density"
    
        query = """
        match (n)
        return n.name as node_name, labels(n) as labels
        order by n.name
        """
    
        df = self.run_query_neo4j(query)
    
        number_nodes = df.shape[0]
    
        query = """
        match (n1)-[r]->(n2)
        return n1.name as node_name_1, labels(n1) as node_1_labels,
               type(r) as relationship_type, n2.name as node_name_2, labels(n2) as node_2_labels
        order by node_name_1, node_name_2
        """
    
        df = self.run_query_neo4j(query)
    
        number_relationships = df.shape[0]

        density = 0.0
        try:
            density = (2 * number_relationships) / (number_nodes * (number_nodes - 1))
        except:
            pass

        df = pd.DataFrame(columns=['nodes', 'relationships', 'density'])
        df['nodes'] = [number_nodes]
        df['relationships'] = [number_relationships]
        df['density'] = [f'{density:.4f}']

        return df

    
    def nodes_relationships(self):
        "print all the nodes and relationships"
    
        print("-------------------------")
        print("  Nodes:")
        print("-------------------------")
    
        query = """
        match (n)
        return n.name as node_name, labels(n) as labels
        order by n.name
        """
    
        df = self.run_query_pandas(query)
    
        number_nodes = df.shape[0]
    
        display(df)
    
        print("-------------------------")
        print("  Relationships:")
        print("-------------------------")
    
        query = """
        match (n1)-[r]->(n2)
        return n1.name as node_name_1, labels(n1) as node_1_labels,
               type(r) as relationship_type, n2.name as node_name_2, labels(n2) as node_2_labels
        order by node_name_1, node_name_2
        """
    
        df = self.run_query_pandas(query)
    
        number_relationships = df.shape[0]
    
        display(df)
    
        density = (2 * number_relationships) / (number_nodes * (number_nodes - 1))
    
        print("-------------------------")
        print("  Density:", f'{density:.1f}')
        print("-------------------------")
    
    
    def create_relationship_one_way(self, from_station, to_station, weight):
        "create a relationship one way between two stations with a weight"
    
        query = """
        MATCH (from:Station),
              (to:Station)
        WHERE from.name = $from_station and to.name = $to_station
        CREATE (from)-[:LINK {weight: $weight}]->(to)
        """

        self.session.run(query, from_station=from_station, to_station=to_station, weight=weight)
    
    
    def create_relationship_two_way(self, from_station, to_station, weight):
        "create relationships two way between two stations with a weight"
    
        query = """
    
        MATCH (from:Station),
              (to:Station)
        WHERE from.name = $from_station and to.name = $to_station
        CREATE (from)-[:LINK {weight: $weight}]->(to),
               (to)-[:LINK {weight: $weight}]->(from)
    
        """
    
        self.session.run(query, from_station=from_station, to_station=to_station, weight=weight)
    
    
    def calculate_box(self, point, miles):
        "Given a point and miles, calculate the box in form left, right, top, bottom"
    
        geod = Geodesic.WGS84
    
        kilometers = miles * 1.60934
        meters = kilometers * 1000
    
        g = geod.Direct(point[0], point[1], -90, meters)
        left = (g['lat2'], g['lon2'])
    
        g = geod.Direct(point[0], point[1], 90, meters)
        right = (g['lat2'], g['lon2'])
    
        g = geod.Direct(point[0], point[1], 0, meters)
        top = (g['lat2'], g['lon2'])
    
        g = geod.Direct(point[0], point[1], 180, meters)
        bottom = (g['lat2'], g['lon2'])
    
        return(left, right, top, bottom)
    
    
    def station_get_zips(self, station, miles):
        
        self.connection.rollback()
        
        query = "select latitude, longitude from stations where station = '" + station + "'"
        
        self.cursor.execute(query)
        
        self.connection.rollback()
        
        rows = self.cursor.fetchall()
        
        for row in rows:
            latitude = row[0]
            longitude = row[1]
            
        point = (latitude, longitude)
            
        (left, right, top, bottom) = self.calculate_box(point, miles)
        
        query = "select zip, population from zip_codes "
        query += " where latitude >= " + str(bottom[0])
        query += " and latitude <= " + str(top[0])
        query += " and longitude >= " + str(left[1])
        query += " and longitude <= " + str(right[1])
        query += " order by 1 "
    
        self.cursor.execute(query)
        self.connection.rollback()
        
        rows = self.cursor.fetchall()
        
        zips = []
        populations = []
        total_populations = []
        total_population = 0
        for row in rows:
            zip = row[0]
            population = row[1]
            total_population += population
            if population > 0.0:
                zips.append(zip)
                populations.append(population)
                total_populations.append(total_population)
        
        return zips, populations, total_populations


    def shortest_path(self, from_station, to_station):
        "given a from station and to station, run and print the shortest path"
        self.clear_graph()
    
        query = "CALL gds.graph.project('ds_graph', 'Station', 'LINK', {relationshipProperties: 'weight'})"
        self.session.run(query)
    
        query = """
    
        MATCH (source:Station {name: $source}), (target:Station {name: $target})
        CALL gds.shortestPath.dijkstra.stream(
            'ds_graph',
            { sourceNode: source,
              targetNode: target,
              relationshipWeightProperty: 'weight'
            }
        )
        YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
        RETURN
            gds.util.asNode(sourceNode).name AS from,
            gds.util.asNode(targetNode).name AS to,
            totalCost,
            [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS nodes,
            costs
        ORDER BY index
    
        """
    
        result = self.session.run(query, source=from_station, target=to_station)
    
        for r in result:
    
            total_cost = int(r['totalCost'])
    
            print("\n--------------------------------")
            print("   Total Cost: ", total_cost)
            print("   Minutes: ", round(total_cost / 60.0,1))
            print("--------------------------------")
    
            nodes = r['nodes']
            costs = r['costs']
    
            i = 0
            previous = 0
    
            for n in nodes:
    
                print(n + ", " + str(int(costs[i]) - previous)  + ", " + str(int(costs[i])))
    
                previous = int(costs[i])
                i += 1
   
class GraphTraversal(StationManager):
    def __init__(self, weight):
        super().__init__(weight)

    def get_node_list(self):
        query = "match (n) return n.name as name"
        result = self.session.run(query)

        node_list = []

        for r in result:
            node_list.append(r["name"])

        node_list = sorted(node_list)
        return node_list

    def plot(self, x, y, count):
        import seaborn as sns
        import matplotlib.pyplot as plt
        plt.figure(figsize=(16,9))
        sns.barplot(x=x, y=y, data=self.df.sort_values(by=y, ascending=False).head(10))
        plt.xticks(rotation=45)

    def plot_map(self):
        import pathlib
        import matplotlib.pyplot as plt
        import geopandas as gpd
        import contextily as ctx

        df = self.get_stations()
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
        df1 = gpd.read_file(pathlib.Path('./BART_System_2020/BART_Track.geojson'))
        df2 = gdf.set_crs(epsg=4326)
        df3 = df1.overlay(df2, how="union", keep_geom_type=False)
        ax = df3.plot(alpha=0.9, cmap='tab10', figsize=(15,20), linewidth=4)
        ctx.add_basemap(ax, crs=df3.crs.to_string(), source=ctx.providers.CartoDB.Positron)
        ax.set_axis_off()

        return self

    def plot_point(self, address):
        import geopandas as gpd
        import geopandas as gpd
        from geopy.geocoders import Nominatim

        geolocator = Nominatim(user_agent="w205")
        result = geolocator.geocode(address)
        if result is not None:
            df = pd.DataFrame(columns=['address', 'longitude', 'latitude'])
            df['latitude'] = result.latitude
            df['longitude'] = result.longitude
            df['address'] = result.address
            self.point = gpd.GeoDataFrame(resultdf, geometry=gpd.points_from_xy(resultdf.longitude, resultdf.latitude))
            self.point = self.point.set_crs(epsg=4326)
            return self.plot(alpha=0.9, cmap='tab10', figsize=(20,25), linewidth=4)


class CentralityAlgos(GraphTraversal):
    def __init__(self, weight):
        super().__init__(weight)

    def degree_centrality(self):
        self.clear_graph()
        query = "CALL gds.graph.project('ds_graph', 'Station', 'LINK', {relationshipProperties: $weight})"
        self.session.run(query, weight=self.weight)

        query = """

        CALL gds.degree.stream('ds_graph')
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).name AS name, score as degree
        ORDER BY degree DESC, name
        """
        self.df = self.run_query_neo4j(query)
        return self;


    def closeness_centrality(self):
        self.clear_graph()
        query = "CALL gds.graph.project('ds_graph', 'Station', 'LINK', {relationshipProperties: $weight})"
        self.session.run(query, weight=self.weight)
        
        query = """
        CALL gds.beta.closeness.stream('ds_graph')
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).name AS name, score as closeness
        ORDER BY score DESC
        """

        self.df = self.run_query_neo4j(query)
        return self;

    def betweenness_centrality(self):
        self.clear_graph()
        query = "CALL gds.graph.project('ds_graph', 'Station', 'LINK', {relationshipProperties: $weight})"
        self.session.run(query, weight=self.weight)

        query = """
        CALL gds.betweenness.stream('ds_graph')
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).name AS name, score as betweenness
        ORDER BY betweenness DESC
        """

        self.df = self.run_query_neo4j(query)
        return self;

    def page_rank(self, max_iterations=20, damping_factor=0.05):
        self.clear_graph()
        query = "CALL gds.graph.project('ds_graph', 'Station', 'TRACK', {relationshipProperties: $weight})"
        self.session.run(query, weight=self.weight)
    
        query = """
        CALL gds.pageRank.stream('ds_graph',
                         { maxIterations: $max_iterations,
                           dampingFactor: $damping_factor}
                         )
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).name AS name, score as page_rank
        ORDER BY page_rank DESC, name ASC
        """

        self.df = self.run_query_neo4j(query, max_iterations=max_iterations, damping_factor=damping_factor)
        return self;

    def page_rank_from_a_node(self, source, max_iterations=20, damping_factor=0.05):
        self.clear_graph()
        query = "CALL gds.graph.project('ds_graph', 'Station', 'TRACK', {relationshipProperties: $weight})"
        self.session.run(query, weight=self.weight)

        query = """

        MATCH (siteA:Station {name: $source})
        CALL gds.pageRank.stream('ds_graph', {
        maxIterations: $max_iterations,
        dampingFactor: $damping_factor,
        sourceNodes: [siteA]
        })
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).name AS name, score as page_rank
            ORDER BY score DESC, name ASC
        """
        self.df = self.run_query_neo4j(query, source=source, max_iterations=max_iterations, damping_factor=damping_factor)
        return self

class CommunityDetectionAlgos(GraphTraversal):
    def __init__(self, weight):
        super().__init__(weight)

    def clustering_coeff_connected_undirected(self, node):
        self.clear_graph()
        query = "CALL gds.graph.project('ds_graph', $node , {KNOWS: {orientation: 'UNDIRECTED'}})"
        self.session.run(query, node=node)

        query = """
        CALL gds.localClusteringCoefficient.stream('ds_graph')
        YIELD nodeId, localClusteringCoefficient
        RETURN gds.util.asNode(nodeId).name AS name, localClusteringCoefficient as clustering_coefficient
        ORDER BY localClusteringCoefficient DESC, name
        """
        self.df = self.run_query_neo4j(query)
        return self;

class FeatureEngineering(GraphTraversal):
    def __init__(self, weight): 
        super().__init__(weight)
        self.table_name = "graphy_features"

    def get_graphy_info(self):
        rollback_before_flag = True
        rollback_after_flag = True

        query = """
        select *
        from graphy_features
        order by node
        """
        self.select_query_pandas(query, rollback_before_flag, rollback_after_flag)

    def create_features_table(self):
        self.connection.rollback()

        query = """
        drop table if exists {table};

        create table {table}(
            node varchar(32),
            degree numeric(5),
            closeness numeric(5,4),
            betweenness numeric(5),
            triangle_count numeric(5),
            clustering_coefficient numeric(5,4),
            community numeric(5)
        );
        """.format(table=self.table_name)

        self.cursor.execute(query)
        self.connection.commit()

        query = """
        insert into graphy_features
        values
        (%s, 0, 0, 0, 0, 0, 0);

        """

        node_list = self.get_node_list()

        for node in node_list:
            self.cursor.execute(query, (node,))

        self.connection.commit()

    def degree_centrality_and_graphy(self):
        self.clear_graph()
        query = "CALL gds.graph.project('ds_graph', 'Station', 'TRACK', {relationshipProperties: $weight})"
        self.session.run(query, weight=self.weight)

        query = """
        CALL gds.degree.stream('ds_graph')
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).name AS name, score as degree
        ORDER BY degree DESC, name
        """

        result = self.run_query_neo4j(query)

        for r in result:
            query = "update graphy_features set degree = %s where node = %s"
            self.cursor.execute(query, (r["degree"], r["name"]))

        self.connection.commit()


class GraphAlgos(CentralityAlgos, CommunityDetectionAlgos, FeatureEngineering):
    def __init__(self, weight): 
        super().__init__(weight)


    def get_best_kitchen_locations(self):
        "Determine the closeness centrality for the given graph"
        self.closeness_scores = self.closeness_centrality().df

        top_scores = []
        top_stations = []

        for index, row in self.closeness_scores.iterrows():
            top_scores.append(float(row['closeness']))
            top_stations.append(row['name'])
        
        self.df = pd.DataFrame(top_stations, columns=['station'])
        self.df['closeness'] = top_scores
        return self.df


    def get_best_commuter_locations(self):
        "return the stations with the top betweenness centrality"
        self.betweenness_scores = self.betweenness_centrality().df

        top_scores = []
        top_stations = []

        for index, row in self.betweenness_scores.iterrows():
            top_scores.append(float(row['betweenness']))
            top_stations.append(row['name'])
    
        self.df = pd.DataFrame(top_stations, columns=['station'])
        self.df['betweenness'] = top_scores
        return self.df


    def get_best_delivery_locations(self):
        "return the stations with the top customer density locations"
        self.degree_scores = self.degree_centrality().df

        top_scores = []
        top_stations = []

        for index, row in self.degree_scores.iterrows():
            top_scores.append(float(row['degree']))
            top_stations.append(row['name'])
    
        self.df = pd.DataFrame(top_stations, columns=['station'])
        self.df['degree centrality'] = top_scores
        return self.df


    def get_avg_stddev(self):
        degree = [row['degree'] for index, row in self.degree_scores.iterrows()]
        avg_degree_centrality = round(statistics.mean(degree),2)
        std_degree_centrality = round(statistics.stdev(degree),2)

        betweenness = [row['betweenness'] for index, row in self.betweenness_scores.iterrows()]
        avg_betweenness_centrality = round(statistics.mean(betweenness),2)
        std_betweenness_centrality = round(statistics.stdev(betweenness),2)

        closeness = [row['closeness'] for index, row in self.closeness_scores.iterrows()]
        avg_closeness_centrality = round(statistics.mean(closeness),2)
        std_closeness_centrality = round(statistics.stdev(closeness),2)

        headers = ["", "Degree Centrality ", " Closeness Centrality ", " Betweenness Centrality"]
        labels = ["Average", "StdDev"]
        data = [["Average", avg_degree_centrality, avg_closeness_centrality, avg_betweenness_centrality],
               ["Standard Deviation", std_degree_centrality, std_closeness_centrality, std_betweenness_centrality]]
        format_row = "{:>21}" * (len(headers) + 1)
        print(format_row.format("", *headers))
        print(format_row.format("", *data[0]))
        print(format_row.format("", *data[1]))

