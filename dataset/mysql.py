# -*- coding: utf-8 -*-

from sqlalchemy import create_engine, Table, Column, Integer, Boolean, BigInteger, Float, String, MetaData, ForeignKey, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    userid = Column("userid", Integer, primary_key=True)
    name = Column("name", String(20))
    age = Column("age", Integer)
    gender = Column("gender", String(2))
    occupation = Column("occupation", Integer)
    zipcode = Column("zipcode", String(20))

class Movie(Base):
    __tablename__ = "movies"
    movieid = Column("movieid", Integer, primary_key=True)
    title = Column("title", String(100))
    genre = Column("genre", String(60))
    avgrating = Column("avgrating", Float)

class Rating(Base):
    __tablename__ = "ratings"
    idx = Column("idx", Integer, primary_key=True)
    userid = Column("userid", Integer)
    movieid = Column("movieid", Integer)
    rating = Column("rating", Integer)
    timestamp = Column("timestamp", BigInteger)

class Auth(Base):
    __tablename__ = "auths"
    userid = Column("userid", Integer, primary_key=True)
    username = Column("username", String(20))
    password = Column("password", String(20))

class Claim(Base):
    __tablename__ = "claims"
    idx = Column("idx", Integer, primary_key=True)
    userid = Column("userid", Integer)
    level = Column("level", Integer)
    isadmin = Column("isadmin", Boolean)

def readDat(filePath):
    data = []
    with open(filePath) as file:
        for line in file:
            line = line.strip().split("::")
            data.append(line)
    return data

def InitAuthDB():
    engine = create_engine("mysql+pymysql://root:123456@localhost:3306/recsys", echo=True)
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)
    auth = Auth(userid=1, username="user1", password="123456")
    session.add(auth)
    session.commit()

def InitClaimsDB():
    engine = create_engine("mysql+pymysql://root:123456@localhost:3306/recsys", echo=True)
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)
    claim = Claim(userid=1, level=5, isadmin=True)
    session.add(claim)
    session.commit()

def movieToDB():
    movies = readDat("./movies.dat")
    engine = create_engine("mysql+pymysql://root:123456@localhost:3306/recsys", echo=True)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    for item in movies:
        movie = Movie(movieid=item[0], title=item[1], genre=item[2], avgrating=0)
        session.add(movie)
    session.commit()

def userToDB():
    users = readDat("./users.dat")
    engine = create_engine("mysql+pymysql://root:123456@localhost:3306/recsys", echo=True)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    for item in users:
        user = User(userid=item[0], name="user" + str(item[0]), gender=item[1], age=item[2],\
                    occupation=item[3], zipcode=item[4])
        session.add(user)
    session.commit()

def ratingToDB():
    ratings = readDat("./ratings.dat")
    engine = create_engine("mysql+pymysql://root:123456@localhost:3306/recsys", echo=True)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    i = 0
    for item in ratings:
        rating = Rating(userid=item[0], movieid=item[1], rating=item[2], timestamp=item[3])
        session.add(rating)
        i += 1
    session.commit()

    result = session.query(Rating.movieid, func.avg(Rating.rating)).group_by(Rating.movieid).all()
    for item in result:
        session.query(Movie).filter(Movie.movieid == item[0]).update({"avgrating": item[1]})
    session.commit()

if __name__ == "__main__":
    InitAuthDB()
    InitClaimsDB()
