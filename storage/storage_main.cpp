#include "global.h"
#include "msg_queue.h"
#include "transport.h"
#include "ycsb.h"
#include "tpcc.h"
#include "work_queue.h"
#include "thread.h"
#include "data_thread.h"
#include "log_thread.h"
#include "io_thread.h"

void parser(int argc, char * argv[]);

void * run_thread(void * id) {
    Thread * thd = (Thread *) id;
	thd->run();
	return NULL;
}

int main(int argc, char *argv[]) {
    printf("Running storage...\n\n");
    // 0. initialize global data structure
    parser(argc, argv);
    assert(g_node_id >= g_node_cnt + g_client_node_cnt && g_node_id < g_node_cnt + g_client_node_cnt + g_storage_node_cnt);
#if SEED != 0
	uint64_t seed = SEED + g_node_id;
#else
	uint64_t seed = get_sys_clock();
#endif
    srand(seed);
    printf("Random seed: %ld\n",seed);

    int64_t starttime;
    int64_t endtime;
    starttime = get_server_clock();
    // per-partition malloc
    printf("Initializing stats... ");
    fflush(stdout);
    stats.init(g_this_total_thread_cnt);
    printf("Done\n");
    printf("Initializing transport manager... ");
    fflush(stdout);
    tport_man.init();
    printf("Done\n");
    printf("Initializing simulation... ");
    fflush(stdout);
    simulation = new SimManager;
    simulation->init();
    printf("Done\n");
    if (g_storage_all_in_one || g_node_id < g_node_cnt + g_client_node_cnt + g_storage_log_node_cnt) {
        printf("Initializing logger... ");
        fflush(stdout);
        logger.init("logfile.log", "txnfile.log");
        printf("Done\n");
    }
    Workload * m_wl;
    if (g_node_id >= g_node_cnt + g_client_node_cnt + g_storage_log_node_cnt) {
        printf("Initializing storage manager... ");
        fflush(stdout);
        switch (WORKLOAD) {
            case YCSB:
                m_wl = new YCSBWorkload;
                break;
            case TPCC:
                m_wl = new TPCCWorkload;
                break;
            default:
                assert(false);
        }
        m_wl->init();
        printf("workload initialized!\n");
    }
    printf("Initializing work queue... ");
    fflush(stdout);
    work_queue.init();
    printf("Done\n");
    printf("Initializing message queue... ");
    fflush(stdout);
    msg_queue.init();
    printf("Done\n");

    endtime = get_server_clock();
    printf("Initialization Time = %ld\n", endtime - starttime);
    fflush(stdout);
    starttime = get_server_clock();

    uint64_t thd_id = 0;
    uint64_t thd_cnt = g_storage_thread_cnt;
    uint64_t lthd_cnt = g_storage_log_thread_cnt;
	uint64_t rthd_cnt = g_storage_rem_thread_cnt;
	uint64_t sthd_cnt = g_storage_send_thread_cnt;
    uint64_t all_thd_cnt = thd_cnt + rthd_cnt + sthd_cnt;
    printf("all_thd_cnt = %ld, g_this_total_thread_cnt = %d\n", all_thd_cnt, g_this_total_thread_cnt);

    assert(all_thd_cnt == g_this_total_thread_cnt);

    pthread_t *p_thds = new pthread_t[all_thd_cnt];
    pthread_barrier_init(&warmup_bar, NULL, all_thd_cnt);

#if SET_AFFINITY
	uint64_t start_cpu = 4;
	cpu_set_t cpus;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
#endif
    
#if STORAGE_ALL_IN_ONE
    LogThread * log_thds = new LogThread[lthd_cnt];
    for (uint64_t i = 0; i < lthd_cnt; i++) {
#if SET_AFFINITY
        CPU_ZERO(&cpus);
        CPU_SET(start_cpu, &cpus);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
        start_cpu++;
#endif
        assert(thd_id < lthd_cnt);
        log_thds[i].init(thd_id, g_node_id, NULL);
        pthread_create(&p_thds[thd_id++], &attr, run_thread, (void *)&log_thds[i]);
    }

    DataThread * data_thds = new DataThread[thd_cnt - lthd_cnt];
    for (uint64_t i = 0; i < thd_cnt - lthd_cnt; i++) {
#if SET_AFFINITY
        CPU_ZERO(&cpus);
        CPU_SET(start_cpu, &cpus);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
        start_cpu++;
#endif
        assert(thd_id >= lthd_cnt && thd_id < thd_cnt);
        data_thds[i].init(thd_id, g_node_id, NULL);
        pthread_create(&p_thds[thd_id++], &attr, run_thread, (void *)&data_thds[i]);
    }
#else
    if (g_node_id >= g_node_cnt + g_client_node_cnt + g_storage_log_node_cnt) {
        //data node
        data_thds = new DataThread[thd_cnt];
        for (uint64_t i = 0; i < thd_cnt; i++) {
#if SET_AFFINITY
            CPU_ZERO(&cpus);
            CPU_SET(start_cpu, &cpus);
            pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
            start_cpu++;
#endif
            assert(thd_id < thd_cnt);
            data_thds[i].init(i);
            pthread_create(&p_thds[i], &attr, run_thread, (void *)&data_thds[i]);
        }
    } else {
        //log node
        log_thds = new LogThread[lthd_cnt];
        for (uint64_t i = 0; i < lthd_cnt; i++) {
#if SET_AFFINITY
            CPU_ZERO(&cpus);
            CPU_SET(start_cpu, &cpus);
            pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
            start_cpu++;
#endif
            assert(thd_id < lthd_cnt);
            log_thds[i].init(i);
            pthread_create(&p_thds[i], &attr, run_thread, (void *)&log_thds[i]);
        }
    }
#endif
    InputThread * input_thds = new InputThread[rthd_cnt];
    for (uint64_t j = 0; j < rthd_cnt ; j++) {
		assert(thd_id >= thd_cnt && thd_id < thd_cnt + rthd_cnt);
		input_thds[j].init(thd_id,g_node_id,m_wl);
		pthread_create(&p_thds[thd_id++], NULL, run_thread, (void *)&input_thds[j]);
	}
    OutputThread * output_thds = new OutputThread[sthd_cnt];
	for (uint64_t j = 0; j < sthd_cnt; j++) {
		assert(thd_id >= thd_cnt + rthd_cnt && thd_id < all_thd_cnt);
		output_thds[j].init(thd_id,g_node_id,m_wl);
		pthread_create(&p_thds[thd_id++], NULL, run_thread, (void *)&output_thds[j]);
	}
    for (uint64_t i = 0; i < all_thd_cnt; i++) {
        pthread_join(p_thds[i], NULL);
    }
    endtime = get_server_clock();
    printf("PASS! SimTime = %f\n", (float)(endtime - starttime) / BILLION);
    fflush(stdout);
    if (STATS_ENABLE) stats.print_storage(false);
    printf("\n");
    fflush(stdout);
    // [ ]: 写磁盘版本的数据清理，日志文件清理
    m_wl->index_delete_all();
    return 0;
}
    