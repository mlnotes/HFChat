import tornado.ioloop
import tornado.web
import time
import logging
import redis
import cgi
import os

from tornado.options import define, options


define("port", default=8888, help="run on the given port", type=int)


class MessageBuffer(object):
	def __init__(self):
		self.waiters = {}
		self.cache_size = 100
		self.r = redis.Redis("localhost", 6379)

	def get_mailbox_id(slef, uid):
		mid = "TO#%s" % uid
		return mid

	def wait_for_messages(self, uid, callback):
		mid = self.get_mailbox_id(uid)
		msg_count = self.r.llen(mid)

		if msg_count > 0:
			msgs = self.r.lrange(mid, 0, self.cache_size)
			for i in range(len(msgs)):
				msgs[i] = eval(msgs[i])	
			callback(msgs)
			
			self.r.delete(mid)
			return

		# currently, no new message, so just wait
		self.waiters[mid] = callback

	def cancel_wait(self, uid):
		mid = self.get_mailbox_id(uid)
		if mid in self.waiters:
			del self.waiters[mid]

	def new_messages(self, messages):
		for msg in messages:
			mid = self.get_mailbox_id(msg['to'])
			
			if mid in self.waiters:
				# send new messages to corresponding users
				callback = self.waiters[mid]
				try:
					callback(messages)	
				except:
					logging.error("Error in waiter callback", exc_info=True)
			
				del self.waiters[mid]

			else:
				# send message to mailbox
				self.r.rpush(mid, msg)
				if self.r.llen(mid) > self.cache_size:
					self.r.lpop()

			# store recent contacts into user's set
			self.r.sadd("U#%s" % msg['from'], msg['to'])
			self.r.sadd("U#%s" % msg['to'], msg['from'])


# global buffer
global_message_buffer = MessageBuffer()

class BaseHandler(tornado.web.RequestHandler):
	def get_current_user(self):
		return self.get_argument("uid")

class Chat(tornado.web.RequestHandler):
	def get(self):
		self.render("index.php")

class MessageNewHandler(BaseHandler):
	def post(self):
		message = {
			"id": time.time(),
			"from": self.get_current_user(), 
			"to": self.get_argument("to"),
			"body": cgi.escape(self.get_argument("body"))
		}

#		self.write({"id": message["id"]})
		self.write(message)
		global_message_buffer.new_messages([message])
		

class MessageUpdatesHandler(BaseHandler):
	@tornado.web.asynchronous
	def post(self):
		global_message_buffer.wait_for_messages(
			self.get_current_user(),
			self.on_new_messages)

	def on_new_messages(self, messages):
		if self.request.connection.stream.closed():
			return
		
		self.finish(dict(messages=messages))

	def on_connection_close(self):
		global_message_buffer.cancel_wait(self.get_current_user())

def main():
	tornado.options.parse_command_line()
	app = tornado.web.Application([
		(r"/", Chat),
		(r"/a/message/new", MessageNewHandler),
		(r"/a/message/updates", MessageUpdatesHandler)
		],
		template_path=os.path.join(os.path.dirname(__file__), "."),
		static_path=os.path.join(os.path.dirname(__file__), "."),
	)

	app.listen(options.port)
	tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
	main()
