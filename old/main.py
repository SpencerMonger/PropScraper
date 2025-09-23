import scrapy
from urllib.parse import urljoin, parse_qs, urlparse
from scrapy.crawler import CrawlerProcess
import json
import mysql.connector
from datetime import datetime, timezone
from mysql.connector import Error


class EasyAvisoSpider(scrapy.Spider):
    conn = mysql.connector.connect(
        host="localhost",
        user="user",
        password="password",
        database="satorihome.mx",
    )

    totalResultados = 0
    name = "easyaviso"
    numpagina = 0

    #se separan en dos los tipos de inmuebles para poder actualizar de manera independiente
    start_urlsDepas = [
        ("venta", "NL",
         "https://www.pincali.com/inmuebles/departamentos-en-venta-en-nuevo-leon?currency_id=10&sort_by=date_activated-desc&min_price=$min&max_price=$max"),
        ("renta", "NL",
         "https://www.pincali.com/inmuebles/departamentos-en-renta-en-nuevo-leon?currency_id=10&sort_by=date_activated-desc&min_price=$min&max_price=$max"),
        ("venta", "alvaro-obregon",
         "https://www.pincali.com/inmuebles/departamentos-en-venta-en-alvaro-obregon-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max"),
        ("renta", "alvaro-obregon",
         "https://www.pincali.com/inmuebles/departamentos-en-renta-en-alvaro-obregon-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max"),
        ("venta", "benito-juarez",
         "https://www.pincali.com/inmuebles/departamentos-en-venta-en-benito-juarez-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max"),
        ("renta", "benito-juarez",
         "https://www.pincali.com/inmuebles/departamentos-en-renta-en-benito-juarez-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max"),
        ("venta", "cuauhtemoc",
         "https://www.pincali.com/inmuebles/departamentos-en-venta-en-cuauhtemoc-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max"),
        ("renta", "cuauhtemoc",
         "https://www.pincali.com/inmuebles/departamentos-en-renta-en-cuauhtemoc-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max"),
        ("venta", "gustavo-a-madero",
         "https://www.pincali.com/inmuebles/departamentos-en-venta-en-gustavo-a-madero-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max"),
        ("renta", "gustavo-a-madero",
         "https://www.pincali.com/inmuebles/departamentos-en-renta-en-gustavo-a-madero-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max"),
        ("venta", "miguel-hidalgo",
         "https://www.pincali.com/inmuebles/departamentos-en-venta-en-miguel-hidalgo-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max"),
        ("renta", "miguel-hidalgo",
         "https://www.pincali.com/inmuebles/departamentos-en-renta-en-miguel-hidalgo-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max")
    ]

    start_urlsCasas = [
        ("venta", "NL",
         "https://www.pincali.com/inmuebles/casas-en-venta-en-nuevo-leon?currency_id=10&sort_by=date_activated-desc&min_price=$min&max_price=$max"),
        ("renta", "NL",
         "https://www.pincali.com/inmuebles/casas-en-renta-en-nuevo-leon?currency_id=10&sort_by=date_activated-desc&min_price=$min&max_price=$max"),
        ("venta", "alvaro-obregon",
         "https://www.pincali.com/inmuebles/casas-en-venta-en-alvaro-obregon-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max"),
        ("renta", "alvaro-obregon",
         "https://www.pincali.com/inmuebles/casas-en-renta-en-alvaro-obregon-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max"),
        ("venta", "benito-juarez",
         "https://www.pincali.com/inmuebles/casas-en-venta-en-benito-juarez-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max"),
        ("renta", "benito-juarez",
         "https://www.pincali.com/inmuebles/casas-en-renta-en-benito-juarez-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max"),
        ("venta", "cuauhtemoc",
         "https://www.pincali.com/inmuebles/casas-en-venta-en-cuauhtemoc-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max"),
        ("renta", "cuauhtemoc",
         "https://www.pincali.com/inmuebles/casas-en-renta-en-cuauhtemoc-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max"),
        ("venta", "gustavo-a-madero",
         "https://www.pincali.com/inmuebles/casas-en-venta-en-gustavo-a-madero-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max"),
        ("renta", "gustavo-a-madero",
         "https://www.pincali.com/inmuebles/casas-en-renta-en-gustavo-a-madero-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max"),
        ("venta", "miguel-hidalgo",
         "https://www.pincali.com/inmuebles/casas-en-venta-en-miguel-hidalgo-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max"),
        ("renta", "miguel-hidalgo",
         "https://www.pincali.com/inmuebles/casas-en-renta-en-miguel-hidalgo-ciudad-de-mexico?currency_id=10&min_price=$min&max_price=$max")
    ]

    # Rangos de precios definidos
    price_ranges_venta = [
        (2500000, 4000000),
        (4000001, 5000000),
        (5000001, 6000000),
        (6000001, 7000000),
        (7000001, 9000000),
        (9000001, 15000000),
        (15000001, 30000000),
        (30000001, 500000000),
    ]

    price_ranges_renta = [
        (25000, 40000),
        (40001, 500000),
        (50001, 1000000),
    ]
    #Cambiar dependiendo del tipo de propiedad a Crawlear
    #tipoPropiedad = "Departamento"
    tipoPropiedad = "Casa"

    def start_requests(self):

        tipoPropiedad = self.tipoPropiedad
        self.desactiva_propiedades(tipoPropiedad)

        for tipo, nombre, url in self.start_urlsCasas: #Cambiar variable start_urlsCasas o start_urlsDepas
            rangos = self.price_ranges_venta
            meta = {
                'tipo': tipo,
                'nombre': nombre
            }
            #filename = f"{tipo}-{nombre}.json"
            #with open(filename, 'a', encoding='utf-8') as f:
                #f.write('[\n')
            if tipo == "renta":
                rangos = self.price_ranges_renta

            for min_price, max_price in rangos:
                urlFiltros = url.replace("$min", str(min_price)).replace("$max", str(max_price))
                self.logger.info("url: " + urlFiltros)
                print("pagina : " + str(urlFiltros))
                yield from self.process_price_range(urlFiltros, meta)
                #yield scrapy.Request(url2.strip(), callback=self.parse)


    def process_price_range(self, url, meta):
        # Primera página del rango actual
        yield scrapy.Request(url, callback=self.parsePrincipal, meta=meta)


    def parsePrincipal(self, response):
        self.numpagina = 1
        html_bytes = response.body
        tipo = response.meta['tipo']
        nombre = response.meta['nombre']
        totalRes = response.css('div.action__result h2::TEXT').get().split()[0].replace(',', '').strip()
        self.totalResultados += int(totalRes)
        self.logger.info("total resultados: " + totalRes + " - " + str(response.request.url))
        self.logger.info("Acumulado de resultados: " + str(self.totalResultados))
        #pagina = response.css('span.pagination-button.page.current::text').get()
        #pagina = response.css('nav.pagination span.current::TEXT').get().split()[0].replace(',', '').strip()
        #pagina3 = response.xpath('//span[contains(@class,"current")]/text()').get()
        pagina2 = response.css('span.pagination-summary::text').get()
        pagina = response.css('span.current::text').get()

        if pagina:
            pagina = pagina.strip()
        else:
            pagina = 1
        print("pagina # : " + str(pagina))
        moneda = 2
        # Extrae propiedades y sigue a cada enlace
        for prop in response.css('li.property__component'):
            property_url = prop.css('a::attr(href)').get()
            precio = prop.css('li.price::TEXT').get()
            if precio:
                precio = precio.strip()
            else:
                precio = 0

            if (precio.find("US$")>=0):
                precio = precio.strip()
                moneda = 1
                precio = precio.replace("US$", "")
            precio = precio.replace(",", "").replace("$", "")
            lat = prop.css('::attr(data-lat)').get()
            lng = prop.css('::attr(data-long)').get()
            print("a1")
            if property_url:
                existeUrl = self.existepropiedadUrl(property_url)
                meta = {
                    'tipo': tipo,
                    'nombre': nombre,
                    'lat': lat,
                    'lng': lng,
                    'pagina': pagina,
                    'existepropiedadUrl': existeUrl,
                }
                print(meta["existepropiedadUrl"])

                if not meta["existepropiedadUrl"]:
                    print("no existe url2: " + property_url)
                    yield scrapy.Request(url = urljoin(response.url, property_url), callback = self.parse_property, meta = meta)
                else:
                    self.activa_propiedad(property_url, self.numpagina, precio, moneda)
                    print("ya existe2: " + property_url)

        # Paginación
        next_page = response.css('span.next.page.pagination-button a::attr(href)').get()
        if next_page:
            meta = {
                'tipo': tipo,
                'nombre': nombre,
            }
            self.numpagina += 1
            self.logger.info("pagina #" + str(self.numpagina))
            print(str(self.numpagina))
            yield response.follow(next_page, callback=self.parsePagina,meta = meta)
            #yield scrapy.Request(url=urljoin(response.url, next_page), callback=self.parsePagina)


    def parsePagina(self, response):
        # Extrae propiedades y sigue a cada enlace
        self.logger.info("pagina #" + str(self.numpagina))
        for prop in response.css('li.property__component'):
            tipo = response.meta['tipo']
            nombre = response.meta['nombre']
            property_url = prop.css('a::attr(href)').get()
            lat = prop.css('::attr(data-lat)').get()
            lng = prop.css('::attr(data-long)').get()
            precio = prop.css('li.price::TEXT').get()
            moneda = 2
            if precio:
                precio = precio.strip()
            else:
                precio = 0

            if (precio.find("US") >= 0):
                precio = precio.strip()
                moneda = 1
                precio = precio.replace("US", "")
            precio = precio.replace(",", "").replace("$", "")
            pagina = response.css('nav.pagination span.current::TEXT').get().split()[0].replace(',', '').strip()
            print("a2")
            if property_url:
                print("pagina #2: " + str(pagina))
                existeUrl = self.existepropiedadUrl(property_url)
                meta = {
                    'tipo': tipo,
                    'nombre': nombre,
                    'lat': lat,
                    'lng': lng,
                    'pagina': pagina,
                    'existepropiedadUrl': existeUrl,
                }
                print(meta["existepropiedadUrl"])

                if not meta["existepropiedadUrl"]:
                    print("no existe url1: " + property_url)
                    yield scrapy.Request(url = urljoin(response.url, property_url), callback = self.parse_property, meta = meta)
                else:
                    self.activa_propiedad(property_url, self.numpagina, precio, moneda)
                    print("ya existe1: " + property_url)

        # Paginación
        next_page = response.css('span.next.page.pagination-button a::attr(href)').get()
        if next_page:
            tipo = response.meta['tipo']
            nombre = response.meta['nombre']
            meta = {
                'tipo': tipo,
                'nombre': nombre,
            }
            self.numpagina += 1
            self.logger.info("pagina #" + str(self.numpagina))
            yield response.follow(next_page, callback=self.parsePagina,meta = meta)
            #yield scrapy.Request(url=urljoin(response.url, next_page),callback=self.parsePagina)


    def activa_propiedad(self, url, pagina, precio, moneda):
        url = "https://www.pincali.com" + url
        cursor = self.conn.cursor()
        fecha_update = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # actualiza en la tabla
        query = ("update bolsa_inmobiliaria_crawler set status = 1,precio={},moneda={}, pagina={}, updated_at='{}' "
                 "where url = '{}'".format(precio, moneda, pagina,fecha_update, url))
        cursor.execute(query)

        query = ("update bolsa_inmobiliaria_propiedades set status = 1, page={}, updated_at='{}' "
                 "where url = '{}'".format( pagina, fecha_update, url))
        cursor.execute(query)
        self.conn.commit()
        cursor.close()


    def desactiva_propiedades(self,tipo):
        if self.conn.is_connected():
            cursor = self.conn.cursor()
        else:
            print("no connection")

        # actualiza en la tabla
        query = "update bolsa_inmobiliaria_crawler set status = 0 where tipo = '{}'".format(tipo)
        cursor.execute(query)
        self.conn.commit()
        cursor.close()


    def parse_property(self, response ):
        # Extrae coordenadas del mapa
        #lat, lng = None, None
        tipo = response.meta['tipo']
        nombre = response.meta['nombre']
        lat = response.meta['lat']
        lng = response.meta['lng']
        pagina = response.meta['pagina']
        existepropiedadUrl = response.meta['existepropiedadUrl']

        id_prop = response.css('.listing-id span::text').get('').replace('ID: ','').strip()
        print("Procesando:" + str(id_prop))

        map_url = response.css('.map-container div::attr(data-lazy-iframe-url)').get()
        locationtxt = response.css('.map-address-info::text').get('').strip()
        locationTipo = response.css('h2.location::text').get('').strip().split()[0]

        if map_url:
            query = parse_qs(urlparse(map_url).query)
            center = query.get('q', [None])[0]
            if center:
                lat2, lng2 = center.split(',')

        # Extrae imágenes de la galería
        images = []
        for img in response.css('div.property__gallery div.picture img'):
            img_url = img.css('::attr(src)').get()
            if img_url and 'placeholder' not in img_url:  # Filtra placeholders
                images.append(urljoin(response.url, img_url))

        # Datos del publicador
        publisher = {
            'name': response.css('.publisher-name::text').get('').strip(),
            'organization': response.css('.publisher-organization-name::text').get('').strip(),
            'phones': [phone.strip() for phone in response.css('.publisher-phones::text').getall()],
        }

        bed = 0
        bath = 0
        car = 0
        cube = 0
        piso = 0
        expand = 0
        date_build = None
        for icon in response.css('div.listing__features div.feature-icon'):
            if icon.css('i::attr(class)').get() == "fal fa-bed":
                bed = icon.css('::TEXT')[1].get().replace('\n', '').strip()

            if icon.css('i::attr(class)').get() == "fal fa-bath":
                bath = icon.css('::TEXT')[1].get().replace('\n', '').strip()

            if icon.css('i::attr(class)').get() == "fal fa-car":
                car = icon.css('::TEXT')[1].get().replace('\n', '').strip()

            if icon.css('i::attr(class)').get() == "fal fa-cube":
                cube = icon.css('::TEXT')[1].get().replace('\n', '').strip()

            if icon.css('i::attr(class)').get() == "fal fa-building":
                piso = icon.css('::TEXT')[1].get().replace('\n', '').strip()

            if icon.css('i::attr(class)').get() == "fal fa-expand":
                expand = icon.css('::TEXT')[1].get().replace('\n', '').strip()

            if icon.css('i::attr(class)').get() == "fal fa-calendar":
                date_build = icon.css('::TEXT')[1].get().replace('\n', '').strip()

        amenidades = {}
        for divAmenidades in response.css('div.listing__amenities div.amenities-group'):
            tipoAmenidad = divAmenidades.css('div.amenities-group-title::TEXT').get().strip()
            for amenidadGrupo in divAmenidades.css('div.amenities-list'):
                amenidadesLista = {}
                i = 0
                for amenidadLi in amenidadGrupo.css('li'):
                    i = i + 1
                    claseAmenidad = amenidadLi.css('div::attr(class)').get().strip()
                    textoAmenidad = amenidadLi.css('span::TEXT').get().strip()
                    liAmenidad = {"icono": claseAmenidad, "texto": textoAmenidad }
                    amenidadesLista[i]=liAmenidad
            amenidades[tipoAmenidad] = amenidadesLista

        #Obtengo el precio
        precio = response.css('div.listing__price div.price div.digits::TEXT').get()
        moneda = 2
        if (precio.find("US$")>=0):
            moneda = 1
            precio = precio.replace("US$", "")
        precio = precio.replace(",", "").replace("$", "")

        operacion = response.css('div.listing__price div.price div.operation-type::TEXT').get().replace("En ",'')
        titulo = response.css('h1::text').get('').strip()
        preveenta = 0
        if "PREVENTA" in titulo.upper():
            preveenta = 1

        # Construye el JSON final
        datos = {
            "id": id_prop,
            "titulo": titulo,
            "preveenta": preveenta,
            "precio": precio,
            "operacion": operacion,
            "tipo": locationTipo,
            "ubicacion": {
                "direccion": locationtxt,
                "lat": lat,
                "lng": lng,
            },
            "caracteristicas": {
                "recamaras": bed,
                "bannos": bath,
                "estacionamientos": car,
                "m2_contruccion": cube,
                "m2_terreno": expand,
                "date_build": date_build,
                "piso": piso,
            },
            'Amenidades': amenidades,
            "images": images,
            "publisher": publisher,
            "url": response.url,
            "origen": "EasyAviso",
            "descripcion": " ".join(response.css('.text-description ::text').getall()).strip(),
        }
        datos_json = json.dumps(datos, ensure_ascii=False)
        fecha = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        cursor = self.conn.cursor()
        tipoPropiedad = self.tipoPropiedad

        query = """
        INSERT INTO bolsa_inmobiliaria_crawler 
        (id_propiedad, datos, procesado, created_at, precio, moneda, operacion, tipo, url, pagina)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (id_prop, datos_json, False, fecha, precio, moneda, operacion, tipoPropiedad, response.url, pagina))
        self.conn.commit()
        cursor.close()


    def existepropiedadUrl(self, url_propiedad):
        try:
            # Configuración de la conexión a la base de datos
            connection = mysql.connector.connect(
                host='localhost',  # Cambia por tu host
                database='satorihome.mx',  # Cambia por el nombre de tu BD
                user='root',  # Cambia por tu usuario
                password=''  # Cambia por tu contraseña
            )

            if connection.is_connected():
                cursor = connection.cursor()
                urlcompleta= "https://www.pincali.com" + url_propiedad
                # Consulta SQL para verificar si existe el id_propiedad
                query = """SELECT COUNT(id) FROM bolsa_inmobiliaria_crawler 
                          WHERE url = %s"""
                cursor.execute(query, (urlcompleta,))
                # Si count es mayor que 0, existe el registro
                existe = cursor.fetchone()[0] > 0
                return existe

        except Error as e:
            print(f"Error al conectar a MySQL: {e}")
            return False

        finally:
            # Asegurarse de cerrar la conexión
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()


    def existepropiedad(self, id_propiedad_buscar):
        try:
            # Configuración de la conexión a la base de datos
            connection = mysql.connector.connect(
                host='localhost',  # Cambia por tu host
                database='satorihome.mx',  # Cambia por el nombre de tu BD
                user='root',  # Cambia por tu usuario
                password=''  # Cambia por tu contraseña
            )

            if connection.is_connected():
                cursor = connection.cursor()

                # Consulta SQL para verificar si existe el id_propiedad
                query = """SELECT COUNT(1) FROM bolsa_inmobiliaria_crawler 
                          WHERE id_propiedad = %s"""

                cursor.execute(query, (id_propiedad_buscar,))

                # Si count es mayor que 0, existe el registro
                existe = cursor.fetchone()[0] > 0
                return existe

        except Error as e:
            print(f"Error al conectar a MySQL: {e}")
            return False

        finally:
            # Asegurarse de cerrar la conexión
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()


settings ={
    'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'FEED_FORMAT': 'json',
    #'FEED_URI': 'properties_full.json',
    'CONCURRENT_REQUESTS': 15,
    'DOWNLOAD_DELAY': 1,
    'LOG_FILE' : "log.txt"
}

# Configuración con parámetros dinámicos
process = CrawlerProcess(settings = settings)
process.crawl(EasyAvisoSpider)
process.start()