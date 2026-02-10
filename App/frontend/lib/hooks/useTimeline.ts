"use client";

import { useQuery } from "@tanstack/react-query";
import { TimelineCluster } from "@/lib/services/timeline";

async function fetchTimeline(): Promise<TimelineCluster[]> {
    const response = await fetch("/api/timeline");
    if (!response.ok) {
        throw new Error("Failed to fetch timeline");
    }
    return response.json();
}

export function useTimeline() {
    return useQuery({
        queryKey: ["timeline"],
        queryFn: fetchTimeline,
    });
}
