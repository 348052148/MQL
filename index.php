<?php

$mql = new SDF\Db\MQL();

var_dump($mql->execute('select * from db.table'));