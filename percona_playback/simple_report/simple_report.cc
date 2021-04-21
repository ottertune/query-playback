/* BEGIN LICENSE
 * Copyright (C) 2011-2013 Percona Ireland Ltd.
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2, as published
 * by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranties of
 * MERCHANTABILITY, SATISFACTORY QUALITY, or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program.  If not, see <http://www.gnu.org/licenses/>.
 * END LICENSE */

#include <cstdio>
#include <cstdlib>
#include <iostream>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <boost/foreach.hpp>
#include <boost/program_options.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include <tbb/tbb_stddef.h>

/* We're all conditional here because TBB in CentOS 6 is old */
#if TBB_VERSION_MAJOR < 3
#include <boost/unordered_map.hpp>
#include <tbb/mutex.h>
#else
#include <tbb/concurrent_unordered_map.h>
#endif

#include <tbb/atomic.h>
#include <percona_playback/plugin.h>
#include <percona_playback/query_result.h>

#include <list>
#include <tbb/concurrent_queue.h>

#include <vector>
#include <algorithm>    // std::sort
#include <math.h>       // sqrt
#include <fstream>

#include <iostream>
#include <ctime>
#include <string>

class DistributionStatistics {
  private:
    long long count;
    double average;
    double standardDeviation;
    long percentiles[8];

    static const int MINIMUM = 0;
    static const int PERCENTILE_25TH = 1;
    static const int MEDIAN = 2;
    static const int PERCENTILE_75TH = 3;
    static const int PERCENTILE_90TH = 4;
    static const int PERCENTILE_95TH = 5;
    static const int PERCENTILE_99TH = 6;
    static const int MAXIMUM = 7;
    const double PERCENTILES[8] = { 0.0, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0 };

  public:

    void computeStatistics(std::vector<long> & values) {
      std::sort(values.begin(), values.end());
      count = values.size();
      if (count == 0) {
        //cannot compute statistics for an empty list
        return;
      }
      for (int i = 0; i < 8; i++) {
        long long index = (long long)(count - 1) * PERCENTILES[i];
        percentiles[i] = values[index];
      }

      double sum = 0;
      for (int i = 0; i < count; i++) {
        sum += values[i];
      }
      average = sum / count;

      double sumDiffsSquared = 0;
      for (int i = 0; i < count; i++) {
        double v = values[i] - average;
        sumDiffsSquared += v * v;
      }

      if (count > 1) {
        standardDeviation = sqrt(sumDiffsSquared / (count - 1));
      }
    }

    std::string get_summary_str() {

      std::ostringstream oss;
      oss << "            # Query  : " << count << std::endl;
      oss << "Latency Average(us)  : " << average << std::endl;
      oss << " Latency StdDev(us)  : " << standardDeviation << std::endl;
      oss << "Latency Minimum(us)  : " << percentiles[MINIMUM] << std::endl;
      oss << "    Latency P25(us)  : " << percentiles[PERCENTILE_25TH] << std::endl;
      oss << " Latency Median(us)  : " << percentiles[MEDIAN] << std::endl;
      oss << "    Latency P75(us)  : " << percentiles[PERCENTILE_75TH] << std::endl;
      oss << "    Latency P90(us)  : " << percentiles[PERCENTILE_90TH] << std::endl;
      oss << "    Latency P95(us)  : " << percentiles[PERCENTILE_95TH] << std::endl;
      oss << "    Latency P99(us)  : " << percentiles[PERCENTILE_99TH] << std::endl;
      oss << "Latency Maximum(us)  : " << percentiles[MAXIMUM] << std::endl;

      return oss.str();
    }

};



class QueryReport {

  private:
    uint64_t thread_id;
    long duration_us;  // microseconds

  public:
    void set_thread_id(uint64_t _thread_id) {
      thread_id = _thread_id;
    }

    uint64_t get_thread_id() {
      return thread_id;
    }

    void set_duration_us(long _duration_us) {
      duration_us = _duration_us;
    }

    long get_duration_us() {
      return duration_us;
    }
};


class SimpleReportPlugin : public percona_playback::ReportPlugin
{
private:
  tbb::atomic<uint64_t> nr_expected_rows_sent;
  tbb::atomic<uint64_t> nr_actual_rows_sent;
  tbb::atomic<uint64_t> nr_queries_rows_differ;
  tbb::atomic<uint64_t> nr_queries_executed;
  tbb::atomic<uint64_t> nr_error_queries;
  tbb::atomic<uint64_t> total_execution_time_ms;
  tbb::atomic<uint64_t> expected_total_execution_time_ms;
  tbb::atomic<uint64_t> nr_quicker_queries;
  tbb::atomic<uint64_t> nr_slower_queries;

  boost::posix_time::ptime start_time;
  boost::posix_time::ptime end_time;

#if TBB_VERSION_MAJOR < 3
  tbb::mutex connection_query_count_mutex;
  typedef boost::unordered_map<uint64_t, tbb::atomic<uint64_t> > ConnectionQueryCountMap;
#else
  typedef tbb::concurrent_unordered_map<uint64_t, tbb::atomic<uint64_t> > ConnectionQueryCountMap;
#endif

  typedef std::pair<uint64_t, tbb::atomic<uint64_t> > ConnectionQueryCountPair;
  typedef std::map<uint64_t, uint64_t> SortedConnectionQueryCountMap;
  typedef std::pair<uint64_t, uint64_t> SortedConnectionQueryCountPair;

  ConnectionQueryCountMap connection_query_counts;

  bool show_connection_query_count;
  bool ignore_row_result_diffs;


  tbb::concurrent_queue<QueryReport> query_report_results;
  unsigned int report_interval = 1000000;

public:
  SimpleReportPlugin(std::string _name) : ReportPlugin(_name)
  {
    nr_queries_executed= 0;
    nr_expected_rows_sent= 0;
    nr_actual_rows_sent= 0;
    nr_queries_rows_differ= 0;
    nr_error_queries= 0;
    show_connection_query_count= false;
    ignore_row_result_diffs = false;
    total_execution_time_ms= 0;
    expected_total_execution_time_ms= 0;
    nr_quicker_queries= 0;
    nr_slower_queries= 0;
  }

  virtual boost::program_options::options_description* getProgramOptions() {
    namespace po= boost::program_options;

    static po::options_description simple_report_options(_("Simple Report Options"));
    simple_report_options.add_options()
    ("show-per-connection-query-count",
     po::value<bool>(&show_connection_query_count)->default_value(false)->zero_tokens(),
     _("For each connection, display the number of queries executed."))
     ("ignore-row-result-diffs",
      po::value<bool>(&ignore_row_result_diffs)->default_value(false)->zero_tokens(),
      _("Ignore differences in the number of rows returned."))
     ("report-interval", po::value<unsigned int>(), _("How often should we print the report log"))
    ;

    return &simple_report_options;
  }

  virtual int processOptions(boost::program_options::variables_map &vm) {
    if (!active &&
      (vm.count("show-per-connection-query-count") || vm.count("ignore-row-result-diffs")) )
      {
	fprintf(stderr,
		_("simple_report plugin is not selected, "
		  "you shouldn't use this plugin-related "
		  "command line options\n"));
	return -1;
      }
    if (vm.count("report-interval"))
    {
      report_interval = vm["report-interval"].as<unsigned int>();
    }

    start_time= boost::posix_time::microsec_clock::universal_time();

    return 0;
  }


  virtual void query_execution(const uint64_t thread_id,
			       const std::string &query,
			       const QueryResult &expected,
			       const QueryResult &actual)
  {
    if (actual.getError())
    {
      fprintf(stderr,_("Error query: %s\n"), query.c_str());
      nr_error_queries++;
    }

    {
#if TBB_VERSION_MAJOR < 3
      tbb::mutex::scoped_lock lock(connection_query_count_mutex);
#endif
      tbb::atomic<uint64_t> zero;
      zero= 0;

      std::pair<ConnectionQueryCountMap::iterator, bool> it_pair=
	connection_query_counts.insert(ConnectionQueryCountPair(thread_id, zero));

      (*(it_pair.first)).second++;
    }

    if (!ignore_row_result_diffs && actual.getRowsSent() != expected.getRowsSent())
    {
      /* Skip this if the input log is general log, because it does not have rows information (value is always 0).*/
      if (expected.getRowsSent() > 0) {
        nr_queries_rows_differ++;
        fprintf(stderr, _("Connection %"PRIu64" Rows Sent: %"PRIu64 " != expected %"PRIu64 " for query: %s\n"), thread_id, actual.getRowsSent(), expected.getRowsSent(), query.c_str());
      }
    }

    nr_queries_executed++;
    nr_expected_rows_sent.fetch_and_add(expected.getRowsSent());
    nr_actual_rows_sent.fetch_and_add(actual.getRowsSent());

    total_execution_time_ms.fetch_and_add(actual.getDuration().total_microseconds());

    if (expected.getDuration().total_microseconds())
    {
      expected_total_execution_time_ms.fetch_and_add(expected.getDuration().total_microseconds());
      if (actual.getDuration().total_microseconds() < expected.getDuration().total_microseconds())
        nr_quicker_queries++;
      else
        nr_slower_queries++;
    }

    if (actual.getDuration().total_microseconds() > 0) {
      // query report has the statistics like duration, thread id, etc.
      QueryReport query_report;
      query_report.set_thread_id(thread_id);
      query_report.set_duration_us(actual.getDuration().total_microseconds());
      query_report_results.push(query_report);
    }
    if (nr_queries_executed % report_interval == 0)
      std::cout << nr_queries_executed << " queries are executed" << std::endl;
  }

  virtual void print_report()
  {
    printf(_("Report\n------\n"));
    printf(_("Executed %" PRIu64 " queries\n"), uint64_t(nr_queries_executed));

    boost::posix_time::time_duration total_duration= boost::posix_time::microseconds(total_execution_time_ms);
    boost::posix_time::time_duration expected_duration= boost::posix_time::microseconds(expected_total_execution_time_ms);
    printf(_("Spent %s executing queries versus an expected %s time.\n"),
           boost::posix_time::to_simple_string(total_duration).c_str(),
           boost::posix_time::to_simple_string(expected_duration).c_str()
           );
    printf(_("%"PRIu64 " queries were quicker than expected, %"PRIu64" were slower\n"),
           uint64_t(nr_quicker_queries),
           uint64_t(nr_slower_queries));

    printf(_("A total of %" PRIu64 " queries had errors.\n"),
	   uint64_t(nr_error_queries));
    printf(_("Expected %" PRIu64 " rows, got %" PRIu64 " (a difference of %lld)\n"),
	   uint64_t(nr_expected_rows_sent),
	   uint64_t(nr_actual_rows_sent),
	   llabs(int64_t(nr_expected_rows_sent) - int64_t(nr_actual_rows_sent))
	   );
    printf(_("Number of queries where number of rows differed: %" PRIu64 ".\n"),
	   uint64_t(nr_queries_rows_differ));

    SortedConnectionQueryCountMap sorted_conn_count;
    uint64_t total_queries= 0;

#if TBB_VERSION_MAJOR < 3
    tbb::mutex::scoped_lock lock(connection_query_count_mutex);
#endif

    BOOST_FOREACH(const ConnectionQueryCountPair conn_count,
		  connection_query_counts)
    {
      sorted_conn_count.insert(SortedConnectionQueryCountPair(conn_count.first, uint64_t(conn_count.second)));

      total_queries+= uint64_t(conn_count.second);
    }

    double avg_queries= (double)total_queries / (double)connection_query_counts.size();

    printf("\n");
    printf(_("Average of %.2f queries per connection (%"PRIu64 " connections).\n"),
	   avg_queries, uint64_t(connection_query_counts.size()));
    printf("\n");

    if (show_connection_query_count)
    {
      printf(_("Per Thread results\n------------------\n"));
      printf(_("Conn Id\t\tQueries\n"));
      BOOST_FOREACH(const SortedConnectionQueryCountPair &conn_count,
		    sorted_conn_count)
      {
	printf("%"PRIu64 "\t\t%"PRIu64 "\n",
	       conn_count.first,
	       conn_count.second);
      }

      printf("\n");
    }

    output_data();
  }

  void output_data() {

    end_time= boost::posix_time::microsec_clock::universal_time();
    
    std::list<QueryReport> results;
    QueryReport res;
    while(query_report_results.try_pop(res)) {
      results.push_back(res);
    }
    std::vector<long> latencies;


    std::time_t ts = std::time(NULL);
    std::ostringstream fnames;
    fnames << "replay_results_" << ts << ".txt";

    std::ofstream myfile;
    myfile.open (fnames.str().c_str());

    // output results
    myfile << "Thread ID, Duration(microseconds)" << std::endl;
    for (std::list<QueryReport>::iterator it=results.begin(); it != results.end(); ++it) {
        myfile << it->get_thread_id() << ", " << it->get_duration_us() << std::endl;
        latencies.push_back(it->get_duration_us());
    }
    myfile.close();

    // output summary statistics
    DistributionStatistics dist;
    dist.computeStatistics(latencies);
    std::string summary_str = dist.get_summary_str();
    output_summary(summary_str);

  }

  void output_summary(std::string summary) {

    std::ofstream myfile;

    std::time_t ts = std::time(NULL);
    std::ostringstream fnames;
    fnames << "replay_summary_" << ts << ".txt";
    myfile.open (fnames.str().c_str());

    std::ostringstream final_summary;
    final_summary << summary;
    final_summary << "         Start Time  : " << start_time << std::endl;
    final_summary << "  Replay Start Time  : " << start_time << std::endl;
    final_summary << "    Replay End Time  : " << end_time << std::endl;
    final_summary << "  Replay Total Time  : " << end_time - start_time << std::endl;
    final_summary << "Replay Total Time(s) : " << (end_time - start_time).total_seconds() << std::endl;
    myfile << final_summary.str();
    myfile.close();

    std::cout << "------------------------------------" << std::endl;
    std::cout << "Summary Statistics: " << std::endl;
    std::cout << "------------------------------------" << std::endl;
    std::cout << final_summary.str() << std::endl;
  }

};

static void init_plugin(percona_playback::PluginRegistry &r)
{
  r.add("simple_report", new SimpleReportPlugin("simple_report"));
}

PERCONA_PLAYBACK_PLUGIN(init_plugin);
