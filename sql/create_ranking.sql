drop table ranking;
create table ranking (
	id serial primary key,
	board varchar(20),
	part varchar(20),
	issue smallint,
	rank int,
	song_id int references song,
	bvid varchar(12) references video,
	count smallint,
	point int,
	view int,
	favorite int,
	coin int,
	"like" int,
	view_rank int,
	favorite_rank int,
	coin_rank int,
	like_rank int
);
create index idx_ranking_board_part on ranking(board, part);
create index idx_ranking_issue on ranking(issue);
create index idx_ranking_rank on ranking(rank);
create index idx_ranking_song_id on ranking(song_id);

