
var http = require('http');

/**
 *
 *
 */
var Connection = function(host, port) {
	this.agent = new http.getAgent(host, port);
	this.host = host;
	this.port = port;
}

/**
 *
 *
 */
Connection.prototype.request = function(method, path, callback) {
	var req = http.request({
		host: this.host,
		port: this.port,
		path: path,
		method: method,
		agent: this.agent,
		headers: {
			Connection: "keep-alive"
		}
	}, callback);
	//console.log(req);
	return req;
}

/**
 *
 *
 */
Connection.prototype.exists = function(path, callback) {
	this.request('HEAD', path, function(response) {
		callback(response.statusCode == 200)
	}).end();
}

/**
 *
 *
 */
Connection.prototype.get = function(path, data, callback) {
	
	if (typeof data === "function") {
		callback = data;
		data = null;
	}
	

	
	var req = this.request('GET', path, function(response) {
		var types = [ "application/json", "application/smile" ];
		var type = response.headers['content-type'];
		for (var i in types) {
			if (type.substring(0, types[i].length) === types[i]) {
				type = types[i];
				break;
			}
		}
		
		if (type == "application/json") {
			var buf = "";
			response.on("data", function(data) {
				buf += data;
			}).on("end", function(){
				var obj = JSON.parse(buf);
				callback(obj);
			});
		} else if (type == "application/smile") {
			
		} else {
			
		}
	});
	if (data) {
		var json = JSON.stringify(data);
		req.setHeader("Content-type", "application/json; encoding=utf8");
		req.setHeader("Content-length", json.length);
		req.write(json, "utf8");
	}
	req.end();
	return this;
}

/**
 *
 *
 */
Connection.prototype.put = function(path, data, callback) {
	var req = this.request('PUT', path, function(response) {
		if (response.statusCode >= 200 && response.statusCode < 300) {
			console.log("Error ("+response.statusCode+")!");
			var buf = "";
			response.on("data", function(data){
				buf += data;
			}).on("end", function(){
				console.log("Error was: "+buf);
			})
		}
		callback && callback();
	});
	if (data) {
		var json = JSON.stringify(data);
		req.setHeader('Content-type', "application/json; encoding=utf8");
		req.setHeader("Content-length", json.length)
		req.write(json);
	}
	req.end();
	return this;
}

/**
 *
 *
 */
Connection.prototype.delete = function(path, callback) {
	return this.request('DELETE', path, callback);
}

/**
 *
 *
 */
Connection.prototype.describe = function(done) {
	this.get("/", done);
	return this;
}

/**
 *
 *
 */
Connection.prototype.index = function(name) {
	return new Index(this, name);
}

/**
 *
 *
 */
Connection.prototype.close = function() {
	for (var i = 0; i < this.agent.sockets.length; ++i)
		this.agent.sockets[i].end();
}

/**
 *
 *
 */
var Index = function(connection, name) {
	this.connection = connection;
	this.name = name;
}

/**
 *
 *
 */
Index.prototype.exists = function(done) {
	this.connection.exists(this.path(), done);
	return this;
}

/**
 *
 *
 */
Index.prototype.delete = function(done) {
	this.connection.delete(this.path(), done);
	return this;
}

/**
 * Open a previously closed index; it will go through the normal recovery process.
 * @see http://www.elasticsearch.org/guide/reference/api/admin-indices-open-close.html
 */
Index.prototype.open = function() {
	this.connection.post(this.path()+"/_open");
	return this;
}

/**
 *  Close an index. A closed index has almost no overhead on the cluster (except for maintaining 
 * its metadata), and is blocked for read/write operations.
 * @see http://www.elasticsearch.org/guide/reference/api/admin-indices-open-close.html
 */
Index.prototype.close = function() {
	this.connection.post(this.path()+"/_close");
	return this;
}

/**
 *
 *
 */
Index.prototype.mapping = function(name) {
	return new Mapping(this, name);
}

/**
 *
 *
 */
Index.prototype.path = function() {
	//names.join(",")
	return "/"+this.name;
}

/**
 *
 *
 */
Index.prototype.describe = function(done) {
	this.connection.get(this.path()+"/_settings", done);
}

/**
 * 
 * @see http://www.elasticsearch.org/guide/reference/api/admin-indices-aliases.html
 */
Index.prototype.alias = function(name) {
	this.connection.post("/_aliases", {actions: [
		{ add: { index: this.name, alias: name }}
	]})
}

/**
 *
 *
 */
Index.prototype.find = function(query, callback) {
	this.connection.get(this.path()+"/_search", query, callback);
	return this;
}

var Mapping = function(index, name) {
	this.connection = index.connection;
	this.name = name;
	this.index = index;
}

/**
 *
 *
 */
Mapping.prototype.exists = function(done) {
	this.connection.exists(this.path(), done);
	return this;
}

Mapping.prototype.delete = function(done) {
	this.connection.delete(this.path(), done);
	return this;
}

/**
 *
 *
 */
Mapping.prototype.path = function() {
	return this.index.path() + "/" + this.name;
}

/**
 *
 *
 */
Mapping.prototype.describe = function(done) {
	this.connection.get(this.path() + "/_mapping", done);
}

/**
 *
 *
 */
Mapping.prototype.documents = function() {
	
}

/**
 *
 *
 */
Mapping.prototype.materialize = function(data, done) {
	var props = { };
	props[this.name] = {
		properties: data
	}
	this.connection.put(this.path() + "/_mapping", props, done);
	return this;
}

/**
 *
 *
 */
Mapping.prototype.document = function(name) {
	return new Document(this, name);
}

/**
 *
 *
 */
var Document = function(mapping, id) {
	this.connection = mapping.connection;
	this.mapping = mapping;
	this.id = id;
}

/**
 *
 *
 */
Document.prototype.exists = function(done) {
	this.connection.exists(this.path(), done )
}

/**
 *
 *
 */
Document.prototype.set = function(data) {
	this.connection.put(this.path(), data);
}

/**
 *
 *
 */
Document.prototype.path = function() {
	return this.mapping.path() + "/" + this.id;
}


