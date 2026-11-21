#pragma once

#include "flat_page_iface.h"
#include "flat_sausage_solid.h"
#include "flat_table_stats.h"
#include "flat_row_eggs.h"
#include "util_basics.h"
#include "cache.h"

#include <ydb/library/actors/util/shared_data.h>

#include <util/generic/string.h>
#include <util/system/types.h>
#include <new>

namespace NKikimr {
namespace NTable {

    class TColdPart;
    class TPart;
    class TMemTable;

    using TPageId = NPage::TPageId;
    using TPageOffset = NPage::TPageOffset;
    using TPageLocation = NPage::TPageLocation;
    using EPage = NPage::EPage;

    struct ISaver {
        using TGlobId = NPageCollection::TGlobId;

        virtual void Save(TSharedData raw, NPage::TGroupId groupId) = 0;
        virtual TLargeObj Save(TRowId, ui32 tag, const TGlobId &glob) = 0;
        virtual TLargeObj Save(TRowId, ui32 tag, TArrayRef<const char> blob) = 0;
    };

    class IPageWriter {
    public:
        using EPage = NPage::EPage;
        using TPageId = NPage::TPageId;

        virtual ~IPageWriter() = default;
        virtual TPageLocation Write(TSharedData page, EPage type, ui32 group) = 0;
        virtual TPageId WriteOuter(TSharedData) = 0;
        virtual void WriteInplace(TPageId page, TArrayRef<const char> body) = 0;
        virtual NPageCollection::TGlobId WriteLarge(TString blob, ui64 ref) = 0;
        virtual void Finish(TString overlay) = 0;

        virtual ui32 GetLastWrittenPageId(ui32 group) const noexcept = 0;
    };

    struct IPages {
        using ELargeObj = NTable::ELargeObj;
        using TPart = NTable::TPart;
        using TMemTable = NTable::TMemTable;
        using TPageId = NPage::TPageId;
        using TPageLocation = NPage::TPageLocation;
        using TGroupId = NPage::TGroupId;

        virtual ~IPages() = default;

        class TReadScope {
        public:
            TReadScope(const TReadScope&) = delete;
            TReadScope& operator=(const TReadScope&) = delete;

            ~TReadScope() {
                if (Env_) {
                    Env_->DoEndOpBatch();
                }
            }

        private:
            friend struct IPages;

            explicit TReadScope(IPages* env) noexcept
                : Env_(env)
            {}

            IPages* Env_;
        };

        // Lexical scopes reuse the outer read operation and end before waiting for pages
        [[nodiscard]] TReadScope BeginOpBatch() noexcept {
            return TReadScope(DoBeginOpBatch() ? this : nullptr);
        }

        // Restart an active batch while its RAII scope keeps ownership of cleanup
        void RestartOpBatch() noexcept {
            DoEndOpBatch();
            DoBeginOpBatch();
        }

        struct TResult {
            explicit operator bool() const noexcept
            {
                return bool(Page);
            }

            const TSharedData* operator*() const noexcept
            {
                return Page;
            }

            bool Need;
            const TSharedData *Page;
        };

        virtual TResult Locate(const TMemTable*, ui64 ref, ui32 tag) = 0;
        virtual TResult Locate(const TPart*, ui64 ref, ELargeObj lob) = 0;

        // The scan has finished the current key and no longer needs its history.
        virtual void ReleaseRowHistory() {}

        /**
         * Returns a data page of the column group, or an empty reference if it is not loaded yet
         */
        virtual const TSharedData* TryGetPage(const TPart* part, const TPageLocation& location, TGroupId groupId) = 0;

        /**
         * Returns an index page (flat or b-tree) of the column group, or an empty reference if it is not loaded yet
         *
         * Index pages may be stored in another page collection than data pages of the same group,
         * so the page collection is chosen by the part (TPart::IndexSlot) rather than the caller
         */
        virtual const TSharedData* TryGetIndexPage(const TPart* part, const TPageLocation& location, TGroupId groupId) = 0;

        /**
         * Hook for cleaning up env on DB.RollbackChanges()
         */
        virtual void OnRollbackChanges() {
            // nothing by default
        }

    protected:
        virtual bool DoBeginOpBatch() noexcept {
            return false;
        }

        virtual void DoEndOpBatch() noexcept {}
    };

}
}
