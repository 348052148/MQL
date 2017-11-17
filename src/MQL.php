<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2017/4/29
 * Time: 10:58
 */

/**
 * 本类是适合与 mml 操作 查询复杂度只支持and 和 or () 优先级查询
 * Class MQL
 * @package controllers
 */
namespace SDF\Db;

class MQL{
    /**
     * 执行过程存储
     * @var array
     */
    private $execute_process = array();
    /**
     * 执行sql
     * @param $sql
     * @return \MongoCursor
     */
    public function execute($sql){
        return $this->mapping($this->parseSql($sql));
    }

    /**
     * 分析程序执行过程
     */
    public function explan($sql=""){
        if(!empty($sql)) $this->execute($sql);
        var_dump($this->execute_process);
    }

    public function setState($state_name,$state_val){
        $this->execute_process[$state_name] = $state_val;
    }

    /***
     * select sql 由 select [字段] from table where [条件]
     */
    private function parseSql($sql){

        //todo 记录
        $this->setState("execute_sql",$sql);

        //todo 字符特殊过滤
        $sql_str = trim($sql);
        $sql_str = strtolower($sql_str);
        $op_type = '';
        //todo sql操作类型解析
        if(preg_match("/^select/",$sql_str)){
            $op_type = 'select';
        }
        if(preg_match("/^update/",$sql_str)){
            $op_type = 'update';
        }
        if(preg_match("/^delete/",$sql_str)){
            $op_type = 'delete';
        }
        if(preg_match("/^insert/",$sql_str)){
            $op_type = 'insert';
        }
        switch($op_type){
            case 'select':
                $parse_info = $this->select($sql_str);
                break;
            case 'update':
                $parse_info = $this->update($sql_str);
                break;
            case 'delete':
                $parse_info = $this->delete($sql_str);
                break;
            case 'insert':
                $parse_info = $this->insert($sql_str);
                break;
            default:
                break;
        }
        return $parse_info;
    }

    /**
     * delete func
     * @param $sql
     */
    private function delete($sql){
        //todo 记录
        $this->setState("execute_op",__FUNCTION__);
        preg_match("/delete\\sfrom\\s([.\\s\\S]+?)where\\s([.\\s\\S]*)/",$sql,$match);

        //todo 表名
        $table = trim($match[1]);
        //todo where
        $symbol_arr = array();

        $condition = $match[2];
        if($condition == "1"){

        }else{
            $symbol_arr = $this->mapping_mongo_where($condition);

        }
        return array(
            'type' => 'delete',
            'collection'=>$table,
            'where'=>$symbol_arr,
        );
    }

    /**
     * update func
     * @param $sql
     * @return array
     */
    public function update($sql){
        //todo 记录
        $this->setState("execute_op",__FUNCTION__);
        preg_match("/update\\s([.\\s\\S]+?)set\\s([.\\s\\S]+?)where\\s([.\\s\\S]*)/",$sql,$match);
        //todo set字段
        $set = str_replace("("," ",$match[2]);
        $set = str_replace(")"," ",$set);
        $set = explode(",",$set);

        $sets = array();
        foreach($set as $se){
            $kv = explode("=",trim($se));
            $sets[$kv[0]] = $this->parse_type($kv[1]);
        }
        $this->setState("execute_sets",$sets);
        //todo 表名
        $table = trim($match[1]);
        //todo where
        $symbol_arr = array();

        $condition = $match[3];
        if($condition == "1"){

        }else{
            $symbol_arr = $this->mapping_mongo_where($condition);

        }

        return array(
            'type' => 'update',
            'collection'=>$table,
            'sets'=>$sets,
            'where'=>$symbol_arr,
        );

    }

    /**
     * insert func
     * @param $sql
     * @return array
     */
    private function insert($sql){
        //todo 记录
        $this->setState("execute_op",__FUNCTION__);

        preg_match("/insert\\sinto ([.\\s\\S]+?)\\s([.\\s\\S]+?)values\\s([.\\s\\S]*)/",$sql,$match);
        //todo 选择字段
        $filed = str_replace("("," ",$match[2]);
        $filed = str_replace(")"," ",$filed);
        $filed = explode(",",$filed);
        //todo 表名
        $table = trim($match[1]);
        //todo values
        $values = str_replace("("," ",$match[3]);
        $values = str_replace(")"," ",$values);
        $values = explode(",",$values);
        foreach($values as $key =>$value){
            $values[trim($key)] = $this->parse_type($value);
        }
        $this->setState("execute_values",$values);

        return array(
            'type' => 'insert',
            'collection'=>$table,
            'field'=>$filed,
            'value'=> $values
        );
    }

    /**
     * SELECT func
     * @param $sql
     * @return array
     */
    private function select($sql){
        //todo 是否聚合操作
        $polymer = 'query';

        //todo 记录
        $this->setState("execute_op",__FUNCTION__);


        //todo 临时sql 构建 这里和mysql 构建 temp表有相似之处
        $temp_sql = $sql;

        $order_sql = !preg_match("/order\\sby\\s([.\\s\\S]*)/", $temp_sql, $order) ?  " order by 1" :" order by ".$order[1];

        $temp_sql = preg_replace("/order\\sby\\s([.\\s\\S]*)/","",$temp_sql);

        $group_sql = !preg_match("/group\\sby\\s([.\\s\\S]*)/",$temp_sql,$group) ? " group by 1" : " group by ".$group[1];

        $temp_sql = preg_replace("/group\\sby\\s([.\\s\\S]*)/","",$temp_sql);

        $where_sql = (!preg_match("/where\\s([.\\s\\S]*)/",$temp_sql,$where))? " where 1" : $where[1];

        $temp_sql = preg_replace("/where\\s([.\\s\\S]*)/","",$temp_sql);


        $sql =  $temp_sql.$where_sql.$group_sql.$order_sql;

        preg_match("/select\\s([.\\s\\S]*?)\\sfrom\\s([.\\s\\S]+?)where\\s([.\\s\\S]*)group\\sby\\s([.\\s\\S]+?)order\\sby\\s([.\\s\\S]*)/",$sql,$match);
//        var_dump($match);die;
        //todo 选择字段
        $filed = str_replace("("," ",$match[1]);
        $filed = str_replace(")"," ",$filed);
        $filed = explode(",",$filed);
        //todo 表名
        $table = trim($match[2]);
        //todo 条件
        //筛选操作符
        $symbol_arr = array();

        $condition = trim($match[3]);
        if($condition == "1"){

        }else{
            $symbol_arr = $this->mapping_mongo_where($condition);

        }
        //todo group by
        $group_by_arr = array();
        $group_by = trim($match[4]);
        if($group_by == "1"){

        }else{
            $polymer = 'group';
            $group_by_arr = explode(",",$group_by);
            $group_by_arr = array_map(function($group_value){
                return trim($group_value);
            },$group_by_arr);
        }
        //todo order by
        $order_by_arr = array();
        $order_by = trim($match[5]);
        if($order_by == "1"){

        }else{
            $order_by_arr = explode(",",$order_by);
            $order_by_arr = array_map(function($order_value){
                return trim($order_value);
            },$order_by_arr);
        }
//        var_dump($order_by_arr);die;

        return array(
            'operate' =>$polymer,
            'type' => 'select',
            'collection'=>$table,
            'field'=>$filed,
            'where'=>$symbol_arr,
            'order_by'=>$order_by_arr,
            'group_by'=>$group_by_arr
        );
    }

    /**
     * 映射执行
     * @param $info
     * @return \MongoCursor
     */
    private function mapping($info){
        $mongo_mode =  new MongoCollection($info['collection'],'chat');
        if($info['type'] == 'select'){
            if($info['operate'] == 'query'){
                $where = $info['where'];
                $mongo_cursor = $mongo_mode->find($where,$info['field']);
                if(count($info['order_by']) > 0 ){
                    $sort = array();
                    $sort_arr = array('asc'=>1,'desc'=>-1);
                    foreach($info['order_by'] as $order){
                        $sort_kv = explode(" ",$order);
                        if(isset($sort_kv[1])){
                            $sort[$sort_kv[0]] = $sort_arr[$sort_kv[1]];
                        }else{
                            $sort[$sort_kv[0]] = 1;
                        }

                    }
                    $mongo_cursor->sort($sort);
                }
                return $mongo_cursor;
            }
            //$sum,$avg,$first,$last,$max,$min,$push,$addToSet
            if($info['operate'] == 'group'){
                $group_arr = array();
                foreach($info['group_by'] as $group){
                    $group_arr['_id'][$group] = '$'.$group;
                }
                foreach($info['field'] as $field){
                    $fields = explode(" ",$field);
                    //todo 验证聚合类型 经支持 $sum,$avg,$first,$last,$max,$min,$push,$addToSet
                    $agg_type = array('sum','avg','first','last','max','min','push','addToSet');
                    if(count($fields)> 1){
                        if(in_array($fields[0],$agg_type)){
                            $group_arr[$field] = array('$'.$fields[0]=>'$'.$fields[1]);
                        }else{
                            //todo 未匹配到直接做丢弃搜索该字段处理
                        }
                    }else{
                        $group_arr[$field] = array('$first'=>'$'.$field);
                    }
                }
                $sort = array();
                $sort_arr = array('asc'=>1,'desc'=>-1);
                foreach($info['order_by'] as $order){
                    $sort_kv = explode(" ",$order);
                    if(isset($sort_kv[1])){
                        $sort[$sort_kv[0]] = $sort_arr[$sort_kv[1]];
                    }else{
                        $sort[$sort_kv[0]] = 1;
                    }

                }
                $piple = array(
                    array('$group' => $group_arr),
                    array('$match' => $info['where']),
                    array('$sort' => $sort)
                );
//                var_dump($piple);die;
                return $mongo_mode->aggregate($piple);
            }

        }
        if($info['type'] == 'insert'){
            $data = array_combine($info['field'],$info['value']);
            return $mongo_mode->insert($data);
        }
        if($info['type']== 'update'){
            return $mongo_mode->update($info['where'],array('$set'=>$info['sets']));
        }
        if($info['type']== 'delete'){
            return $mongo_mode->remove($info['where'],array('justOne'=>true));
        }

    }

    /**
     * 构建SQL 查询条件
     * @param $str
     * @return array
     */
    private function mapping_mongo_where($str){
        //todo 记录
        $this->setState("execute_where",$str);

        $str = trim($str);

        $str = str_replace("and", "+", $str);

        $str = str_replace("or", "-", $str);

        $str = str_replace(" ","",$str);
        //todo 迭代最深层次列表
        $arr = array();
        $pre_filter = "";
        while(true){
            $filter = $this->parse($str);
            if($pre_filter == $filter){
                $arr['head'] = $filter;
                break;
            }
            $pre_filter = $filter;
            $key =  md5(rand(0,100));
            $arr[$key]=(explode(',',$filter));
            $str = str_replace("(".$filter.")","$key",$str);
        }
        $this->setState("execute_mapping",$arr);

        $jihe = array();
        //todo 构建规则数组
        $this->head1($arr,'head',$jihe);

//        var_dump($jihe);
        $this->setState("execute_relation",$jihe);

        $query = array();
        //todo 构建Mongo查询数组
        $this->gearate($query,$jihe);

        $this->setState("execute_query",$query);

        return $query;
    }

    /**
     * 解析函数。构建括号层次字符串
     * @param $str
     * @return mixed
     */
    private function parse($str)
    {
        $pre_num = -1;
        $bk_num = -1;
        $p_op = array();
        $b_op = array();
        for ($i = 0; $i < strlen($str); $i++ ) {

            $current_str = substr($str, $i, 1);
            if($current_str == "("){
                array_push($p_op,$current_str);
            }
            if($current_str == ")"){
                array_push($b_op,$current_str);
            }
            if ($pre_num ==-1 && $current_str == "(" ) {
                $pre_num = $i;

            }else if($current_str == ")"){
                $bk_num = $i;
            }
            if($pre_num != -1 && $bk_num != -1 && count($p_op) == count($b_op)){

                return  $this->parse(substr($str,$pre_num+1,$bk_num-$pre_num-1));
            }
        }
        return $str;
    }

    /**
     * 根据关系映射数组解析成mongo查询数组规则
     * @param $query
     * @param $jihe
     */
    private function gearate(&$query,$jihe){
        $temp = "AND";
        for($i=0;$i<count($jihe); $i++){

            $j = $jihe[$i];

            if(isset($j['str'])){
                $pre_type = $j['type'];
                $kv = explode("=",$j['str']);
                if($pre_type == "AND"){
                    $query[$kv[0]] = $this->parse_type($kv[1]);
                    $temp = "AND";
                }
                if($pre_type == "OR"){
                    $temp_arr =array();
                    $temp_arr[$kv[0]] = $this->parse_type($kv[1]);
                    $query['$or'][]=$temp_arr;
                    $temp = "OR";
                }
            }else{
                if($temp == "OR"){
                    $this->gearate($query['$or'][],$j);
                }
                if($temp == "AND"){
                    $this->gearate($query,$j);
                    $temp = "OR";
                }
            }
        }
    }

    /**
     * 规则数组重组
     * @param $arr
     * @param $k
     * @param $jihe
     */
    private function head1($arr,$k,&$jihe){
        if(is_array($arr[$k])){
            $list = $this->arr($arr[$k][0]);
        }else {
            $list = $this->arr($arr[$k]);
        }
        $i =0 ;
        foreach($list as $li){
            if(preg_match("/=/",$li['str'])){
                $jihe[$i]=$li;
            }else{
                $jihe[$i] = array();
                $this->head1($arr,$li['str'],$jihe[$i]);
            }
            $i ++;
        }
    }

    /**
     * 值类型判断
     * @param $val
     * @return int|string
     */
    private function parse_type($val){
        $val = trim($val);
        if(preg_match("/^(\\'|\")([.\\s\\S]*?)(\\'|\")$/",$val)){
            $val = substr($val,1,-1);
        }else{
            $val = intval($val);
        }
        return $val;
    }

    /**
     * 对字符串进行反解析
     * @param $str
     * @return array
     */
    private function arr($str){
        $sum_arr = array();
        $list = explode('+',$str);
        foreach($list as $li){
            if(preg_match("/-/",$li)){
                $lis = explode("-",$li);
                $i=0;
                foreach($lis as $l){
                    if($i == 0){
                        $sum_arr[] = array('str'=>$l,'type'=>'OR');
                    }else{
                        $sum_arr[] = array('str'=>$l,'type'=>'OR');
                    }
                    $i++;
                }
            }else{
                $sum_arr[] = array('str'=>$li,'type'=>'AND');
            }
        }
        return $sum_arr;
    }
}