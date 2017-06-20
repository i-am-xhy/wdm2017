// SC1_by_id:
function (doc) {
  if (doc.type == "movie") {
      emit(doc.movie_id, {idmovies: doc.movie_id, title: doc.title, year: doc.year, keywords: doc.keywords, genres: doc.genres, series: doc.series});
  } else if (doc.type == "actor") {
    doc.acted_in.forEach(function(element) {
      emit(element.idmovies, {idactors: doc.idactors, fname: doc.fname, lname: doc.lname, gender: doc.gender, character: element.character});
    })
  }
}

// SC2_by_id:
function (doc) {
    if (doc.type == "actor") {
        emit(doc.idactors, {idactors: doc.idactors, fname: doc.fname, lname: doc.lname, gender: doc.gender});
        doc.acted_in.forEach(function(element) {
            emit(doc.idactors, element);
        })
    }
}

// SC3_by_id:
function (doc) {
    if (doc.type == "actor") {
        emit(doc.idactors, {idactors: doc.idactors, fname: doc.fname, lname: doc.lname, acted_in_count: doc.acted_in.length});
    }
}

// SC4_by_id:
function (doc) {
    if(doc.type == "movie"){
        doc.genres.forEach(function(genre) {
            if (genre && doc.year) {
                emit([genre, doc.year], {idmovies: doc.movie_id, title: doc.title, year: doc.year});
            }
        })
    }
}

// SC5_by_id:
function (doc) {
    if(doc.type == "movie"){
        doc.genres.forEach(function(genre) {
            if (genre && doc.year) {
                emit([doc.year, genre], 1);
            }
        })
    }
}