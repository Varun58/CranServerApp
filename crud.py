from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import io
import urllib.request
import requests
import tarfile
import os

app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'crud.sqlite')
some_engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])

Session = sessionmaker(bind=some_engine)
session = Session()

db = SQLAlchemy(app)
ma = Marshmallow(app)


class Package(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80), unique=True)
    version = db.Column(db.String(80))
    publishedDate = db.Column(db.String(80))
    title = db.Column(db.String(100))
    description = db.Column(db.String(200))
    authors = db.Column(db.String(80))
    maintainers = db.Column(db.String(80))

    def __init__(self, name, version, publishedDate, title, description, authors, maintainers):
        self.name = name
        self.version = version
        self.publishedDate = publishedDate
        self.title = title
        self.description = description
        self.authors = authors
        self.maintainers = maintainers

    @classmethod
    def find_all_by_name(cls, session, name):
        return session.query(cls).filter(Package.name.like('%' + name + '%')).all()

    @classmethod
    def find_by_name_version(cls, session, name, version):
        return session.query(cls).filter(Package.name == name).filter(Package.version == version).all()


class PackageSchema(ma.Schema):
    class Meta:
        # Fields to expose
        fields = ('name', 'version', 'publishedDate', 'title', 'description', 'authors', 'maintainers')


package_schema = PackageSchema()
packages_schema = PackageSchema(many=True)


# endpoint to create new package
@app.route("/package", methods=["POST"])
def add_package():
    name = request.json['name']
    version = request.json['version']
    publishedDate = request.json['publishedDate']
    title = request.json['title']
    description = request.json['description']
    authors = request.json['authors']
    maintainers = request.json['maintainers']

    new_package = Package(name, version, publishedDate, title, description, authors, maintainers)

    db.session.add(new_package)
    db.session.commit()

    return package_schema.jsonify(new_package)


# endpoint to show all packages
@app.route("/package", methods=["GET"])
def get_package():
    all_packages = Package.query.all()
    result = packages_schema.dump(all_packages)
    return jsonify(result)


# endpoint to get package detail by id
@app.route("/package/<id>", methods=["GET"])
def package_detail(id):
    package = Package.query.get(id)
    return package_schema.jsonify(package)


# endpoint to update package
@app.route("/package/<id>", methods=["PUT"])
def package_update(id):
    package = Package.query.get(id)
    name = request.json['name']
    version = request.json['version']
    publishedDate = request.json['publishedDate']
    title = request.json['title']
    description = request.json['description']
    authors = request.json['authors']
    maintainers = request.json['maintainers']

    package.name = name
    package.version = version
    package.publishedDate = publishedDate
    package.title = title
    package.description = description
    package.authors = authors
    package.maintainers = maintainers

    db.session.commit()
    return package_schema.jsonify(package)


# endpoint to delete package
@app.route("/package/<id>", methods=["DELETE"])
def package_delete(id):
    package = Package.query.get(id)
    db.session.delete(package)
    db.session.commit()

    return package_schema.jsonify(package)


# endpoint to search package detail by name
@app.route("/package/search", methods=["GET"])
def package_search():
    name = request.args['name']
    found = Package.find_all_by_name(session, name)
    return packages_schema.jsonify(found)


# endpoint to import and save packages
@app.route("/package/import", methods=["GET"])
def package_import():
    resp = requests.get('https://cran.r-project.org/src/contrib/PACKAGES')
    lines = resp.text.split('\n')

    package_name = ""
    for x in lines:
        if x.find("Package: ") != -1:
            package_name = x[9:]
        if x.find("Version: ") != -1:
            package_version = x[9:]
            package_file_name = package_name + '_' + package_version
            print("Package Complete Path->" + package_file_name)

            existing_package = Package.find_by_name_version(session, package_name, package_version)
            if not existing_package:
                print("This is a new package, get info from cran server->" + package_file_name)
                tar_url = 'https://cran.r-project.org/src/contrib/' + package_file_name + '.tar.gz'
                description = get_description(tar_url)
                save_package(description)
            package_name = ""

    return 'success'


def save_package(file_description):
    lines = file_description.decode().split('\n')

    name = version = publishedDate = title = description = authors = maintainers = ""
    for x in lines:
        if "Package: " in x:
            name = x[9:]
        elif "Version: " in x:
            version = x[9:]
        elif "Title: " in x:
            title = x[7:]
        elif "Description: " in x:
            description = x[13:]
        elif "Date/Publication: " in x:
            publishedDate = x[18:]
        elif "Author: " in x:
            authors = x[8:]
        elif "Maintainer: " in x:
            maintainers = x[12:]

    new_package = Package(name, version, publishedDate, title, description, authors, maintainers)

    print("search package by name" + name + " and version->" + version)
    searched_package = Package.find_by_name_version(session, name, version)

    if not searched_package:
        print("no previous package found, save this")
        db.session.add(new_package)
        db.session.commit()
    else:
        print("package already exists!")

    return new_package


def get_description(tar_url):
    ftp_stream = urllib.request.urlopen(tar_url)

    temp_file = io.BytesIO()
    while True:
        s = ftp_stream.read(16384)

        if not s:
            break

        temp_file.write(s)
    ftp_stream.close()

    temp_file.seek(0)

    tfile = tarfile.open(fileobj=temp_file, mode="r:gz")

    description_file_list = [filename
                             for filename in tfile.getnames()
                             if "DESCRIPTION" in filename]

    description_file = tfile.extractfile(description_file_list[0])
    description = description_file.read()

    tfile.close()
    temp_file.close()
    return description


if __name__ == '__main__':
    app.run(debug=True)
