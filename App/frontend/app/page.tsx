"use client";

import { useTimeline } from "@/lib/hooks/useTimeline";
import { TimelineCard } from "@/components/timeline-card";

export default function Home() {
  const { data: timeline, isLoading, error } = useTimeline();

  return (
    <main className="min-h-screen px-4 py-8">
      <div className="mx-auto max-w-2xl">
        <header className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900">Nieuwstijdlijn</h1>
          <p className="mt-2 text-gray-600">
            Ontdek hoe Nederlandse nieuwsbronnen nieuwsonderwerpen dekken
          </p>
        </header>

        {isLoading && (
          <div className="flex items-center justify-center py-12">
            <div className="h-8 w-8 animate-spin rounded-full border-4 border-blue-600 border-t-transparent"></div>
          </div>
        )}

        {error && (
          <div className="rounded-lg bg-red-50 p-4 text-red-700">
            Er is een fout opgetreden bij het laden van de tijdlijn.
          </div>
        )}

        {timeline && timeline.length === 0 && (
          <div className="rounded-lg bg-gray-100 p-4 text-gray-600">
            Geen nieuwsclusters gevonden.
          </div>
        )}

        {timeline && timeline.length > 0 && (
          <div>
            {timeline.map((cluster) => (
              <TimelineCard key={cluster.cluster_id} cluster={cluster} />
            ))}
          </div>
        )}
      </div>
    </main>
  );
}
