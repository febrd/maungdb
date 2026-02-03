command     = insert | select ;
insert      = "simpen" table data ;
select      = "tingali" table [ where ] ;
where       = "dimana" field operator value ;
operator    = "=" | "!=" | ">" | "<" | ">=" | "<=" ;
