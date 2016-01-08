package zxy.segment;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

public class segmentReduce extends ReducerBase {
	
	
	private Record output;
	//SESSION中两个ACTION最大间隔，单位秒，用于分割SESSION
	private final long ACTION_MAX_SECONDS=3600;
	//两次action的最短间隔，单位秒，用于过滤爬虫
	private final long ACTION_MIN_SECONDS=0;
	//SESSION的最小长度
	private final long MIN_ACTION_LEN=5;
	//SESSION的最大长度
	private final long MAX_ACTION_LEN=1000;
	@Override
	public void setup(TaskContext context) throws IOException {
		super.setup(context);
		output=context.createOutputRecord();	
	}

	
	//一条记录行
	class user_info implements Comparable<user_info>{
		String user_id;
		Long item_id;
		String category;
		String action;
		String vtime;
		//每个action的操作时间,用于过滤爬虫
		long seeTime;
			
		public int compareTo(user_info temp){
			return this.vtime.compareTo(temp.vtime);
		}
	}
	
	
	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		
		List<user_info> one_user_list=new ArrayList<user_info>();
		
		long session_ID=0;
		while(values.hasNext()){
			Record val=values.next();
			user_info temp=new user_info();
			temp.user_id=key.getString("user_id");
			temp.item_id=val.getBigint("item_id");
			temp.category=val.getString("category");
			temp.action=val.getString("action");
			temp.vtime=val.getString("vtime");
			one_user_list.add(temp);
		}
		
		int len_one_user_list=one_user_list.size();
		
		//一个user的action长度范围
		if(len_one_user_list < MIN_ACTION_LEN || len_one_user_list>MAX_ACTION_LEN )
			return;
		
		//日期排序
		Collections.sort(one_user_list);
		
		//日期转化
		double d1 = 0;
		double d2 = 0;
		String pre_vtime=one_user_list.get(0).vtime;
		try {
			d1 = sdf.parse(pre_vtime).getTime();
		} catch (ParseException e1) {}
		

		for (int i = 0; i < len_one_user_list; i++) {
			try {
				d2=sdf.parse(one_user_list.get(i).vtime).getTime();
				if(i>0)
					one_user_list.get(i-1).seeTime=(long) (d2-d1)/1000;
				d1=d2;
			}catch (Exception e) {}
		}
		
		
		//一个session中的category数目统计
		Map<String,Integer> category_count_map=new HashMap<String, Integer>();
		
		int start=0;
		one_user_list.get(len_one_user_list-1).seeTime=10;
		for (int i = 0; i < len_one_user_list; i++) {
			user_info thistime=one_user_list.get(i);
			if( thistime.seeTime>ACTION_MAX_SECONDS ){
				if( (i-start+1)>MIN_ACTION_LEN){
					//找出主category,如果存在则写入
					//System.out.println((Double.toString((d2-d1)/1000))+","+Integer.toString(i-start));
					String category_this_time=getMainCategory(category_count_map);
					if(category_this_time!=null)
					{
						for(int j=start;j<=i;j++){
							user_info temp_User_info=one_user_list.get(j);
							if(temp_User_info.category.equals(category_this_time)&&temp_User_info.seeTime>=ACTION_MIN_SECONDS)
								write2context(context, temp_User_info, session_ID);
						}
					session_ID+=1;
					}
				}
				category_count_map.clear();
				start=i+1;	
			}
			
			else if(thistime.seeTime>=ACTION_MIN_SECONDS){
				if( category_count_map.containsKey(thistime.category))
					category_count_map.put(thistime.category, category_count_map.get(thistime.category)+1);
				else
					category_count_map.put(thistime.category, 1);	
			}	
		}
		//最后一组特殊处理
		if( (len_one_user_list-start)>MIN_ACTION_LEN){
			String category_this_time=getMainCategory(category_count_map);
			if(category_this_time!=null)
				for(int j=start;j<len_one_user_list;j++){
					user_info temp_User_info=one_user_list.get(j);
					if(temp_User_info.category.equals(category_this_time)&&temp_User_info.seeTime>=ACTION_MIN_SECONDS){
						write2context(context, temp_User_info, session_ID);
						}
				}
			}
		}

	
	//得到主category,数目小于MIN_ACTION_LEN时返回NULL
	private String getMainCategory(Map<String,Integer> category_count_map){
		
		int max_num=0;
		String category_max = null;
		for(String category:category_count_map.keySet()){
			if(category_count_map.get(category)>max_num){
				category_max=category;
				max_num=category_count_map.get(category);
			}
		}	
		if(max_num<MIN_ACTION_LEN)
			return null;
		
		return category_max;	
	}
	
	//写入reduce输出
	private void write2context(TaskContext context,user_info thistime,long session_ID) throws IOException{
		output.set(0,session_ID);
		output.set(1,thistime.user_id);
		output.set(2,thistime.item_id);
		output.set(3,thistime.category);
		output.set(4,thistime.action);
		output.set(5,thistime.vtime);
		context.write(output);	
	}
	
	
}