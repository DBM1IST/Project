----------------------
--CLEAR TABLES-------
----------------------
DROP TABLE if EXISTS users CASCADE;
DROP TABLE if EXISTS relevant_post CASCADE;
DROP TABLE if EXISTS localization CASCADE;
DROP TABLE if EXISTS advertising_company CASCADE;
DROP TABLE if EXISTS influencer CASCADE;
DROP TABLE if EXISTS diffusion_channel CASCADE;
DROP TABLE if EXISTS verified_in CASCADE;


-------------------
--TABLES CREATION--
-------------------


CREATE TABLE users(
    username TEXT NOT NULL,
    name TEXT NOT NULL,
    age INT NOT NULL,
    n_followers INT NOT NULL,
    n_post INT,
    PRIMARY KEY (username)
);

CREATE TABLE relevant_post(
    id_post INT NOT NULL,
    n_coments INT NOT NULL,
    n_like INT NOT NULL,
    type TEXT NOT NULL,
    PRIMARY KEY (id_post)
);

CREATE TABLE localization(
    population INT NOT NULL,
    county TEXT NOT NULL,
    city TEXT NOT NULL,
    PRIMARY KEY (city),
);

CREATE TABLE advertising_company(
    id_company INT NOT NULL,
    deposit INT NOT NULL,
    PRIMARY KEY (id_company),
);

CREATE TABLE influencer(
    username TEXT NOT NULL,
    FOREIGN KEY (username) REFERENCES users (username),
    PRIMARY KEY (username)
);

CREATE TABLE diffusion_channel(
    n_channel INT NOT NULL,
    name_channel TEXT NOT NULL,
    FOREIGN KEY (username) REFERENCES users (username),
    PRIMARY KEY (name_channel)
);

-- The primary key is username and date
CREATE TABLE verified_in(
    date_verified DATE,
    PRIMARY KEY (date_verified, username),
    FOREIGN KEY (username) REFERENCES users(username)
);
