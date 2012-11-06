require 'mysql2'

file_start = <<-TEXT
{"nodes":[
{"name":"All", "group":0},
{"name":"Smooth", "answer_id":1, "group":1},
{"name":"Features or disk", "answer_id":2, "group":3},
{"name":"Star or artifact", "answer_id":3, "group":2},
{"name":"Clumpy", "answer_id":4, "group":3},
{"name":"Not Clumpy", "answer_id":5, "group":3},
{"name":"Bar", "answer_id":6, "group":3},
{"name":"No bar", "answer_id":7, "group":3},
{"name":"Spiral", "answer_id":8, "group":3},
{"name":"No spiral", "answer_id":9, "group":3},
{"name":"No bulge", "answer_id":10, "group":3},
{"name":"Just noticeable", "answer_id":11, "group":3},
{"name":"Obvious", "answer_id":12, "group":3},
{"name":"Dominant", "answer_id":13, "group":3},
{"name":"Completely round", "answer_id":16, "group":1},
{"name":"In between", "answer_id":17, "group":1},
{"name":"Cigar shaped", "answer_id":18, "group":1},
{"name":"Rounded", "answer_id":25, "group":3},
{"name":"Boxy", "answer_id":26, "group":3},
{"name":"No bulge", "answer_id":27, "group":3},
{"name":"Tight spiral", "answer_id":28, "group":3},
{"name":"Medium spiral", "answer_id":29, "group":3},
{"name":"Loose spiral", "answer_id":30, "group":3},
{"name":"1 arm", "answer_id":31, "group":3},
{"name":"2 arms", "answer_id":32, "group":3},
{"name":"3 arms", "answer_id":33, "group":3},
{"name":"4 arms", "answer_id":34, "group":3},
{"name":"More than 4 arms", "answer_id":36, "group":3},
{"name":"Can't tell arms", "answer_id":37, "group":3},
{"name":"Yes", "answer_id":39, "group":4},
{"name":"No", "answer_id":40, "group":4},
{"name":"Yes", "answer_id":41, "group":4},
{"name":"No", "answer_id":42, "group":4},
{"name":"Yes", "answer_id":43, "group":4},
{"name":"No", "answer_id":44, "group":4},
{"name":"Yes", "answer_id":45, "group":4},
{"name":"No", "answer_id":46, "group":4},
{"name":"Straight Line", "answer_id":47, "group":4},
{"name":"Chain", "answer_id":48, "group":4},
{"name":"Cluster", "answer_id":49, "group":4},
{"name":"2 clumps", "answer_id":50, "group":4},
{"name":"3 clumps", "answer_id":51, "group":4},
{"name":"4 clumps", "answer_id":52, "group":4},
{"name":"More than 4 clumpd", "answer_id":53, "group":4},
{"name":"Can't tell clumps", "answer_id":54, "group":4},
{"name":"Yes", "answer_id":55, "group":4},
{"name":"No", "answer_id":56, "group":4},
{"name":"Yes", "answer_id":57, "group":4},
{"name":"No", "answer_id":58, "group":4},
{"name":"Spiral-shape", "answer_id":59, "group":4},
{"name":"1 clumps", "answer_id":60, "group":4}
],
"links":[
TEXT

file_end = "}"

client = Mysql2::Client.new(:host => 'localhost', :database => 'hubble-zoo', :username => "root", :password => "")

start_function = client.query("DROP FUNCTION IF EXISTS getID")

sql = <<-SQL
  CREATE FUNCTION getID(a INT) RETURNS INT
  DETERMINISTIC
  BEGIN
    DECLARE newid INT DEFAULT 0;
    RETURN (SELECT new_id FROM id_trans WHERE old_id=a);
  END
SQL
    
start_function = client.query(sql)

galaxies = [	 21422,   
  20871,   
  21574,   
  21612,   
  20054,   
  21261,   
  21584,   
  20986,   
  21556,   
  21575,   
  21442,   
  21518,   
  21670,   
  19989,   
  21471,   
  16987,   
  21661,   
  21528,   
  21642,   
  21804,   
  19714,   
  21291,   
  20436,   
  21576,   
  21339,   
  15335,   
  21634,   
  20475,   
  21482,   
  20266,   
  20589,   
  20247,   
  19696,   
  20190,   
  21245,   
  21606,   
  20257,   
  21559,   
  21597,   
  21673,   
  20704,   
  15517,   
  21654,   
  20334,   
  21550,   
  21493,   
  21531,   
  15584,   
  21645,   
  20619,   
  21465,   
  20534,   
  19537,   
  20753,   
  20772,   
  21513,   
  21627,   
  20411,   
  21618,   
  21086,   
  21504,   
  21656,   
  20754,   
  21628,   
  20108,   
  21809,   
  20460,   
  20327,   
  21657,   
  21486,   
  21600,   
  20289,   
  20774,   
  21515,   
  21078,   
  14846,   
  21648,   
  21430,   
  21658,   
  21620,   
  21525,   
  21544,   
  21582,   
  21478,   
  21364,   
  21383,   
  21592,   
  21573,   
  20357,   
  15588,   
  20927,   
  20918,   
  21640,   
  21165,   
  21659  
						]

output = {}
galaxies.each do |galaxy|
	
	gal = client.query("SET @galaxyid = #{galaxy}")

	image_url = client.query("SELECT location FROM assets WHERE id = @galaxyid")
	
	sql = <<-SQL
		SELECT annotations.*, COUNT(*) as value FROM annotations, classifications, asset_classifications WHERE classifications.id = asset_classifications.classification_id AND annotations.classification_id = classifications.id AND asset_id=9614 AND answer_id IN (1,2,3) GROUP BY answer_id
	SQL
	top3 = client.query(sql)
	
	sql = <<-SQL
	  SELECT getID(a1.answer_id) as a1, getID(a2.answer_id) as a2, COUNT(*) as value
	  FROM (SELECT annotations.* FROM annotations, classifications, asset_classifications WHERE classifications.id = asset_classifications.classification_id AND annotations.classification_id = classifications.id AND asset_id =@galaxyid) as a1, (SELECT annotations.* FROM annotations, classifications, asset_classifications WHERE classifications.id = asset_classifications.classification_id AND annotations.classification_id = classifications.id AND asset_id =@galaxyid) as a2 WHERE (a1.id = a2.id-1)
	  AND a1.classification_id = a2.classification_id AND a1.answer_id != a2.answer_id AND a1.answer_id NOT IN (14,15,19,20,21,22,23,24,38) AND a2.answer_id NOT IN (14,15,19,20,21,22,23,24,38)
	  GROUP BY CONCAT(a1.answer_id, a2.answer_id)
	  ORDER BY a1.answer_id ASC
	SQL
	
	main = client.query(sql)
	
	topout = top3.map{|r| "{\"source\":0, \"target\":#{r['answer_id']}, \"value\":#{r['value']}}"}.join(",\n")
	mainout = main.map{|r| "{\"source\":#{r['a1']}, \"target\":#{r['a2']}, \"value\":#{r['value']}}"}.join(",\n")
	
	output = topout+",\n"+mainout+"],\n\"image_url\":\""+image_url.first['location']+"\""
	
	
	puts "Galaxy #{galaxy} done"
	File.open("data/#{galaxy}.json", 'w+') {|f| f.write(file_start+output+file_end) }
	
end