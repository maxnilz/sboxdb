## Reference

### CMU Educational Relational Database Management System BusTub

[BusTub](https://github.com/cmu-db/bustub) is a relational database management system built
at [Carnegie Mellon University](https://db.cs.cmu.edu) for
the [Introduction to Database Systems](https://15445.courses.cs.cmu.edu) (15-445/645) course. This system was developed
for educational purposes and should not be used in production environments.

Huge thanks to the public CMU database courses and Professor Andy Pavlo, who made the course public to everyone on
YouTube
and Gradescope

- [CMU 15-445/645 Fall 2022 Database Systems](https://15445.courses.cs.cmu.edu/fall2022/schedule.html)
- [CMU 15-445/645 Fall 2023 Database Systems](https://15445.courses.cs.cmu.edu/fall2023/schedule.html)
- [CMU 15-721 Spring 2023 Advanced Database Systems](https://15721.courses.cs.cmu.edu/spring2023/schedule.html)

And my learning notes and projects:

- [15-446/645 Notes](https://cc.maxnilz.com/docs/003-database/)
- [15-721 Spring 2023 Postgres Foreign Data Wrapper](https://github.com/maxnilz/db721_fdw)

### Postgres

During the implementation of
the [15-721 Spring 2023 Postgres Foreign Data Wrapper](https://github.com/maxnilz/db721_fdw), I got a chance to learn
how [postgres](https://github.com/postgres/postgres) works and implemented roughly

- The overall query execution path.
- Query analysis, planning, and execution.

The module [`access`](../src/access) naming is borrowed from the postgres, **BUT**, just naming, other than that no code
is borrowed from postgres.

### Raft

Huge thanks to the [MIT](https://www.mit.edu/)
Professor [Robert Morris](https://pdos.csail.mit.edu/archive/rtm/) make
course [6.824 distributed system](http://nil.csail.mit.edu/6.824/2022/schedule.html)
public to everyone on YouTube. With the course, I got the chance to learn the distributed system theory systematically.
Of course, the [Raft protocol](http://nil.csail.mit.edu/6.824/2022/papers/raft-extended.pdf) is included in the course.

And my labs implementation is at [here](https://github.com/maxnilz/6.824-golabs-2022)

### toydb

After some time of learning in the database field, I decided to build an experimental database system to test out my
learning. And, since [Rust](http://rustlang.org) is getting popular in both academia and industry. So, building a
distributed database system in Rust as my journey to learning both database and Rust becomes my choice.

Curious about similar efforts, I searched online and discovered [toydb](https://github.com/erikgrinaker/toydb),
which also references the CMU database systems course surprisingly. It turns out `toydb` to be a simple, well-designed
project with clear code, making it especially helpful for Rust newcomers

From both rust-wise and database-wise, `sboxdb` borrowed some ideas from it.

### Datafusion

[DataFusion](https://github.com/apache/datafusion) is an extensible query engine written in `Rust` that
uses [Apache Arrow](https://arrow.apache.org) as its in-memory format. It also ships with
a [standalone SQL parser](https://github.com/apache/datafusion-sqlparser-rs)

### Papers along the way

https://maxnilz.com/papers
