// sc1/by_id:
function (doc) {
    if (doc.type == "movie") {
        emit(doc.movie_id, {
            idmovies: doc.movie_id,
            title: doc.title,
            year: doc.year,
            keywords: doc.keywords,
            genres: doc.genres,
            series: doc.series
        });
    } else if (doc.type == "actor") {
        doc.acted_in.forEach(function (element) {
            emit(element.idmovies, {
                idactors: doc.idactors,
                fname: doc.fname,
                lname: doc.lname,
                gender: doc.gender,
                character: element.character
            });
        })
    }
}

// sc1/title_to_id:
function (doc) {
    if (doc.type == "movie") {
        emit(doc.title, [doc.movie_id, doc.year]);
    }
}

// sc2/by_id:
function (doc) {
    if (doc.type == "actor") {
        emit(doc.idactors, {idactors: doc.idactors, fname: doc.fname, lname: doc.lname, gender: doc.gender});
        doc.acted_in.forEach(function (element) {
            emit(doc.idactors, element);
        })
    }
}

// sc2/title_to_id:
function (doc) {
    if (doc.type == "actor") {
        emit([doc.fname, doc.lname], doc.idactors);
    }
}

// sc3/by_id
function (doc) {
    if (doc.type == "actor") {
        emit(doc.idactors, {
            idactors: doc.idactors,
            fname: doc.fname,
            lname: doc.lname,
            acted_in_count: doc.acted_in.length
        });
    }
}

// sc4/by_id:
function (doc) {
    if (doc.type == "movie") {
        doc.genres.forEach(function (genre) {
            if (genre && doc.year) {
                emit([genre, doc.year], {idmovies: doc.movie_id, title: doc.title, year: doc.year});
            }
        })
    }
}

// sc5/by_id:
// with reducer: _count (_sum works too)
function (doc) {
    if (doc.type == "movie") {
        doc.genres.forEach(function (genre) {
            if (genre && doc.year) {
                emit([doc.year, genre], 1);
            }
        })
    }
}
