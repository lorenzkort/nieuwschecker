import { prisma } from "@/lib/prisma";

const CLUSTER_PUBLISH_DELAY_HOURS = 2;

export interface TimelineCluster {
    cluster_id: number;
    title: string | null;
    articles: string | null;
    num_articles: number | null;
    feeds: string | null;
    num_feeds: number | null;
    min_published_date: Date | null;
    max_published_date: Date | null;
    time_span_hours: number | null;
    missing_feeds: string | null;
    owner_reach: string | null;
    centre_right: number | null;
    centre: number | null;
    centre_left: number | null;
    right: number | null;
    left: number | null;
    cluster_reach: number | null;
    blindspot_left: number | null;
    blindspot_right: number | null;
    single_owner_high_reach: number | null;
    max_published_date_fmt: string | null;
}

export async function getFilteredTimeline(): Promise<TimelineCluster[]> {
    const delayDate = new Date();
    delayDate.setHours(delayDate.getHours() - CLUSTER_PUBLISH_DELAY_HOURS);

    const clusters = await prisma.timeline.findMany({
        where: {
            AND: [
                {
                    OR: [
                        { num_feeds: { gt: 8 } },
                        { blindspot_left: 1 },
                        { blindspot_right: 1 },
                        { single_owner_high_reach: 1 },
                    ],
                },
                {
                    max_published_date: { lt: delayDate },
                },
            ],
        },
        orderBy: {
            max_published_date: "desc",
        },
    });

    // Deduplicate clusters by cluster_id (keep the first occurrence which is the newest due to sort)
    const seenIds = new Set<number>();
    const uniqueClusters = clusters.filter((cluster: { cluster_id: number }) => {
        if (seenIds.has(cluster.cluster_id)) {
            return false;
        }
        seenIds.add(cluster.cluster_id);
        return true;
    });

    return uniqueClusters;
}
