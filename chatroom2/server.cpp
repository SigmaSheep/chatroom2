//
// chat_server.cpp
// ~~~~~~~~~~~~~~~
//
// Copyright (c) 2003-2018 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include <cstdlib>
#include <deque>
#include <iostream>
#include <list>
#include <memory>
#include <set>
#include <utility>
#include <boost/asio.hpp>
#include "chat_message.hpp"


//----------------------------------------------------------------------

typedef std::deque<chat_message> chat_message_queue;

//----------------------------------------------------------------------

class chat_participant
{
public:
	virtual ~chat_participant() {}
	virtual void deliver(const chat_message& msg) = 0;
};

typedef std::shared_ptr<chat_participant> chat_participant_ptr;

//----------------------------------------------------------------------

class chat_room
{
public:
	void join(chat_participant_ptr participant)
	{
		participants_.insert(participant);
		//for (auto msg : recent_msgs_)
			//participant->deliver(msg);
	}

	void leave(chat_participant_ptr participant)
	{
		participants_.erase(participant);
	}

	void deliver(const chat_message& msg)
	{
		recent_msgs_.push_back(msg);
		while (recent_msgs_.size() > max_recent_msgs)
			recent_msgs_.pop_front();

		for (auto participant : participants_)
			participant->deliver(msg);
	}

	void SetMapperNumber(const int num) {
		number_of_mapper_ = num;
	}

	void AddOneFinishedMapper() {
		finished_mapper_++;
		if (finished_mapper_ == number_of_mapper_) {
			chat_message msg;
			char line[25]="AllMappingFinished";
			msg.body_length(std::strlen(line));
			std::memcpy(msg.body(), line, msg.body_length());
			msg.encode_header();
			for (auto participant : participants_)
				participant->deliver(msg);
		}
	}

private:
	std::set<chat_participant_ptr> participants_;
	enum { max_recent_msgs = 100 };
	chat_message_queue recent_msgs_;
	int number_of_mapper_;
	int finished_mapper_=0;
};

//----------------------------------------------------------------------

class chat_session
	: public chat_participant,
	public std::enable_shared_from_this<chat_session>
{
public:
	chat_session(boost::asio::ip::tcp::socket socket, chat_room& room)
		: socket_(std::move(socket)),
		room_(room)
	{
	}

	void start()
	{
		room_.join(shared_from_this());
		do_read_header();
	}

	void deliver(const chat_message& msg)
	{
		bool write_in_progress = !write_msgs_.empty();
		write_msgs_.push_back(msg);
		if (!write_in_progress)
		{
			do_write();
		}
	}

private:
	void do_read_header()
	{
		auto self(shared_from_this());
		boost::asio::async_read(socket_,
			boost::asio::buffer(read_msg_.data(), chat_message::header_length),
			[this, self](boost::system::error_code ec, std::size_t /*length*/)
		{
			if (!ec && read_msg_.decode_header())
			{
				do_read_body();
			}
			else
			{
				room_.leave(shared_from_this());
			}
		});
	}

	void do_read_body()
	{
		auto self(shared_from_this());
		boost::asio::async_read(socket_,
			boost::asio::buffer(read_msg_.body(), read_msg_.body_length()),
			[this, self](boost::system::error_code ec, std::size_t /*length*/)
		{
			if (!ec)
			{
				std::string s(read_msg_.body(), read_msg_.body_length());
				if(s == "map_process_done"){
					room_.AddOneFinishedMapper();
				}
				std::cout.write(read_msg_.body(), read_msg_.body_length());
				std::cout<<s<<"\n";
				room_.deliver(read_msg_);
				do_read_header();
			}
			else
			{
				room_.leave(shared_from_this());
			}
		});
	}

	void do_write()
	{
		auto self(shared_from_this());
		boost::asio::async_write(socket_,
			boost::asio::buffer(write_msgs_.front().data(),
				write_msgs_.front().length()),
			[this, self](boost::system::error_code ec, std::size_t /*length*/)
		{
			if (!ec)
			{
				write_msgs_.pop_front();
				if (!write_msgs_.empty())
				{
					do_write();
				}
			}
			else
			{
				room_.leave(shared_from_this());
			}
		});
	}

	boost::asio::ip::tcp::socket socket_;
	chat_room& room_;
	chat_message read_msg_;
	chat_message_queue write_msgs_;
};

//----------------------------------------------------------------------

class chat_server
{
public:
	chat_server(boost::asio::io_context& io_context,
		const boost::asio::ip::tcp::endpoint& endpoint,
		const int number_of_mapper)
		: acceptor_(io_context, endpoint)
	{
		room_.SetMapperNumber(number_of_mapper);
		do_accept();
	}

private:
	void do_accept()
	{
		acceptor_.async_accept(
			[this](boost::system::error_code ec, boost::asio::ip::tcp::socket socket)
		{
			if (!ec)
			{
				std::make_shared<chat_session>(std::move(socket), room_)->start();
			}

			do_accept();
		});
	}

	boost::asio::ip::tcp::acceptor acceptor_;
	chat_room room_;
};

//----------------------------------------------------------------------

int main()
{
	boost::asio::io_context io_context;
	boost::asio::ip::tcp::endpoint endpoint(
		boost::asio::ip::tcp::v4(), 6060);
	chat_server servers(io_context, endpoint,4);
	io_context.run();


	return 0;
}