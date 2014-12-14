var aiota = require("aiota-utils");
var jsonValidate = require("jsonschema").validate;
var MongoClient = require("mongodb").MongoClient;
var ObjectID = require("mongodb").ObjectID;
var config = require("./config");
var rpc = require("amqp-rpc").factory({ url: "amqp://" + config.amqp.login + ":" + config.amqp.password + "@" + config.amqp.host + ":" + config.amqp.port });

var actionUpdateQueue = [];
var db = null;

function validate(instance, schema)
{
	var v = jsonValidate(instance, schema);

	return (v.errors.length == 0 ? { isValid: true } : { isValid: false, error: v.errors });
}

function updateActions()
{
	if (actionUpdateQueue.length > 0) {
		var update = actionUpdateQueue.shift();
		
		var schema = { 
			type: "object",
			properties: {
				_id: { type: "string", required: true },
				status: { type: "integer", minimum: 0, required: true },
				progress: {
					type: "object",
					properties: {
						timestamp: { type: "integer", minimum: 0, required: true },
						status: { type: "string", required: true }
					},
					required: true
				},
				resendAfter: { type: "integer" }
			}
		};
	
		var v = validate(update, schema, true);
		
		if (v.isValid) {
			db.collection("actions", function(err, collection) {
				if (err) {
					console.log(err);
					sendGETResponse(request, response, { error: "Unable to open actions collection." });
					return;
				}

				var upd = {};
				
				upd["$set"] = { status: update.status };
				upd["$push"] = { progress: update.progress };
				
				if (update.hasOwnProperty("resendAfter")) {
					upd["$set"]["resends.resendAfter"] = update.resendAfter;
				}
				
				if (update.status == 2) {
					// Resend
					var inc = {};
					inc["resends.numResends"] = 1;
					upd["$inc"] = inc;
				}

				collection.update({ _id: new ObjectID(update._id) }, upd, function(err, result) {
					if (err) {
						console.log(err);
					}
					
					process.nextTick(updateActions);
				});
			});
		}
		else {
			console.log(v.error);
			process.nextTick(updateActions);
		}
	}
}

function getDeviceMsg(db, msg, tokens, callback)
{
	var reply = [];
	
	db.collection("actions", function(err, collection) {
		if (err) {
			callback({ error: err });
			return;
		}
		
		var encryptionMethod = "none";
		
		var actions = [];
		
		var stream = collection.find({ deviceId: msg.header.deviceId, "encryption.tokencardId": msg.header.encryption.tokencardId, status: { $lt: 10 } }, { encryption: 1, requestId: 1, action: 1, params: 1, status: 1, timeoutAt: 1, resends: 1 }).sort({ "progress.0.timestamp": 1 }).limit(15).stream();
		
		stream.on("error", function (err) {
			callback({ error: err, errorCode: 200005 });
			return;
		});

		stream.on("data", function(doc) {
			switch (encryptionMethod) {
			case "none":		encryptionMethod = doc.encryption.method;
								break;
			case "hmacsha256":	encryptionMethod = (doc.encryptionMethod == "aes256gcm" ? "aes256gcm" : "hmacsha256");
								break;
			}

			actions.push(doc);
		});

		stream.on("end", function() {
			if (actions.length > 0) {
				var now = Date.now();
				
				for (var i = 0; i < actions.length; ++i) {
					var addToReply = true;
					
					if (actions[i]["timeoutAt"] <= now) {
						// This action has timed out
						actionUpdateQueue.push({ _id: actions[i]["_id"].toString(), progress: { timestamp: now, status: "timed out" }, status: 30 });
						addToReply = false;
					}
					else {
						if (actions[i]["status"] == 0) {
							actionUpdateQueue.push({ _id: actions[i]["_id"].toString(), progress: { timestamp: now, status: "sent to device" }, status: 1, resendAfter: now + actions[i].resends.resendTimeout });
						}
						else {
							// This is a resend
							if (actions[i].resends.numResends < actions[i].resends.maxResends) {
								if (actions[i].resends.resendAfter <= now) {
									// The resend timeout has expired
									actionUpdateQueue.push({ _id: actions[i]["_id"].toString(), progress: { timestamp: now, status: "resent to device" }, status: 2, resendAfter: now + actions[i].resends.resendTimeout });
								}
								else {
									// Don't send this action until the resend timeout expires
									addToReply = false;
								}
							}
							else {
								// We have exhausted the maximum number of resends
								actionUpdateQueue.push({ _id: actions[i]["_id"].toString(), progress: { timestamp: now, status: "max. resends exhausted" }, status: 31 });
								addToReply = false;
							}
						}
					}

					if (addToReply) {
						var action = { action: actions[i]["action"], requestId: actions[i]["requestId"] };
						if (actions[i].hasOwnProperty("params")) {
							action["params"] = actions[i].params;
						}
						reply.push(action);
					}
				}
				
				process.nextTick(updateActions);		
			}
				
			if ((reply.length > 0) || (msg.body.timeout == 0)) {
				var nonce = 0;
				aiota.respond(null, msg.header.deviceId, { group: "response", type: "poll" }, encryptionMethod, msg.header.encryption.tokencardId, tokens, nonce, reply, function(response) {
					callback(response);
				});
			}
			else {
				var timeout = msg.body.timeout * 1000;
				msg.body.timeout = 0;
				setTimeout(getDeviceMsg, timeout, db, msg, tokens, callback);
			}
		});
	});
}

function handleLongPollingRequest(msg, callback)
{
	var schema = { 
		type: "object",
		properties: {
			header: {
				type: "object",
				properties: {
					requestId: { type: "string", required: true },
					deviceId: { type: "string", required: true },
					type: { type: "string", enum: [ "poll" ], required: true },
					timestamp: { type: "integer", minimum: 0, required: true },
					ttl: { type: "integer", minimum: 0, required: true },
					encryption: {
						type: "object",
						properties: {
							method: { type: "string", required: true },
							tokencardId: { type: "string", required: true }
						},
						required: true
					}
				},
				required: true
			},
			"body": {
				type: "object",
				properties: { 
					timeout: { type: "integer", minimum: 0, required: true }
				},
				required: true
			},
			nonce: { type: "integer", minimum: 0, required: true }
		}
	};

	var v = validate(msg, schema);

	if (v.isValid) {
		db.collection("devices", function(err, collection) {
			if (err) {
				callback({ error: err });
				return;
			}
			
			collection.findOne({ _id: msg.header.deviceId }, { _id: 0, apps: 1 }, function(err, device) {
				if (err) {
					callback({ error: err });
					return;
				}
				else {
					if (device) {
						if (device.apps.hasOwnProperty(msg.header.encryption.tokencardId)) {
							var app = device.apps[msg.header.encryption.tokencardId];
							
							schema = {
								type: "object",
								properties: {
									name: { type: "string", required: true },
									version: { 
										type: "object", 
										properties: {
											major: { type: "integer", required: true },
											minor: { type: "integer", required: true }
										},
										required: true
									},
									status: { type: "string", enum: [ "pending", "registered" ], required: true },
									session: {
										type: "object",
										properties: {
											id: { type: "string", required: true },
											timeoutAt: { type: "integer", minimum: 0, required: true }
										},
										required: true
									},
									lastRequest: { type: "integer", minimum: 0, required: true },
								}
							};
			
							v = validate(app, schema);
			
							if (v.isValid) {
								db.collection("applications", function(err, collection) {
									if (err) {
										callback({ error: err, errorCode: 200001 });
										return;
									}
									
									collection.findOne({ _id: msg.header.encryption.tokencardId }, { _id: 0, tokens: 1 }, function(err, appl) {
										if (err) {
											callback({ error: err, errorCode: 200002 });
											return;
										}
										else {
											if (appl) {
												if (appl.hasOwnProperty("tokens")) {
													getDeviceMsg(db, msg, appl.tokens, function(result) {
														callback(result);
													});
												}
												else {
													callback({ error: "The application tokens are not defined.", errorCode: 100023 });
												}
											}
											else {
												callback({ error: "The application is not defined.", errorCode: 100016 });
											}
										}
									});
								});
							}
							else {
								callback({ error: v.error, errorCode: 100003 });
							}				
						}
						else {
							callback({ warning: "This application has not been registered. Please register first.", errorCode: 100024 });
						}
					}
					else {
						callback({ warning: "The device-id does not exist. Please register first.", errorCode: 100025 });
					}
				}
			});			
		});
	}
	else {
		// Invalid long polling message
		callback({ error: v.error, errorCode: 100003 });
	}
}

MongoClient.connect("mongodb://" + config.database.host + ":" + config.database.port + "/aiota", function(err, aiotaDB) {
	if (err) {
		aiota.log(config.processName, config.serverName, aiotaDB, err);
	}
	else {
		MongoClient.connect("mongodb://" + config.database.host + ":" + config.database.port + "/" + config.database.name, function(err, dbConnection) {
			if (err) {
				aiota.log(config.processName, config.serverName, aiotaDB, err);
			}
			else {
				db = dbConnection;
		  
				var cl = { group: "longpolling" };
		
				rpc.on(aiota.getQueue(cl), handleLongPollingRequest(msg, callback));
	
				setInterval(function() { aiota.heartbeat(config.processName, config.serverName, aiotaDB); }, 10000);

				process.on("SIGTERM", function() {
					aiota.terminateProcess(config.processName, config.serverName, db, function() {
						process.exit(1);
					});
				});
			}
		});
	}
});